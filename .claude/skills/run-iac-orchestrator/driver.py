#!/usr/bin/env python3
"""Drive a running lyndrix-core instance with a headless browser.

lyndrix-core is a FastAPI + NiceGUI app: pages are an SPA shell that renders
over a websocket, so `curl` only sees the shell. This driver uses Playwright
(Chromium) to log in and screenshot real, rendered pages at desktop + mobile
widths — the only way to actually see the UI.

Prereqs (see SKILL.md): a venv with `playwright` + `python -m playwright install chromium`.
The target stack must already be running (default http://localhost:8081).

Examples:
    python driver.py                       # login + screenshot the default routes
    python driver.py --routes /plugins     # just the Plugin Manager
    python driver.py --health-only         # no browser; curl /api/health and exit
    python driver.py --base http://localhost:8081 --user admin --password secret
"""
import argparse
import json
import os
import sys
import time
import urllib.request

DEFAULT_ROUTES = ["/", "/dashboard", "/plugins", "/settings"]
DESKTOP = {"width": 1440, "height": 900}
MOBILE = {"width": 390, "height": 844}  # iPhone 12-ish


def http_health(base: str) -> dict:
    with urllib.request.urlopen(f"{base}/api/health", timeout=10) as r:
        return json.load(r)


def slug(route: str) -> str:
    s = route.strip("/").replace("/", "_") or "root"
    return s


def login(page, base: str, user: str, password: str) -> None:
    page.goto(f"{base}/login", wait_until="networkidle")
    page.wait_for_selector("input", timeout=15000)
    time.sleep(0.5)
    pw = page.locator("input[type=password]").first
    user_box = page.locator("input:not([type=password])").first
    user_box.click()
    user_box.fill(user)
    pw.click()
    pw.fill(password)
    # Submit: press Enter, then fall back to clicking a Login button if present.
    pw.press("Enter")
    try:
        page.wait_for_url(lambda u: "login" not in u, timeout=4000)
    except Exception:
        btn = page.get_by_role("button")
        for i in range(btn.count()):
            label = (btn.nth(i).inner_text() or "").strip().lower()
            if any(k in label for k in ("login", "sign in", "anmelden")):
                btn.nth(i).click()
                break
        page.wait_for_url(lambda u: "login" not in u, timeout=8000)
    page.wait_for_load_state("networkidle")
    # NiceGUI stores auth in app.storage.user, which is hydrated via the
    # WebSocket connection. Each new page navigation opens a new WS, and the
    # server must look up the session cookie before main_layout's
    # is_authenticated() check runs. 2 s is enough; 1 s is not reliably enough
    # for /plugins and /settings.
    time.sleep(2.0)


def shoot(page, base: str, route: str, viewport: dict, outdir: str, tag: str) -> str:
    page.set_viewport_size(viewport)
    page.goto(f"{base}{route}", wait_until="networkidle")
    # NiceGUI paints over the socket after load; give it a beat.
    time.sleep(1.5)
    # If main_layout's auth check ran before the WS session was ready, we got
    # redirected to /login. Retry once with a longer wait.
    # (Skip the check for the login page itself — being at /login is correct there.)
    if route != "/login" and "login" in page.url:
        time.sleep(2.0)
        page.goto(f"{base}{route}", wait_until="networkidle")
        time.sleep(2.0)
        if "login" in page.url:
            print(f"warning: {route} redirected to login — screenshot will show login page",
                  file=sys.stderr)
    path = f"{outdir}/{slug(route)}.{tag}.png"
    page.screenshot(path=path, full_page=True)
    return path


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--base", default=os.environ.get("LYNDRIX_BASE", "http://localhost:8081"))
    ap.add_argument("--user", default=os.environ.get("LYNDRIX_ADMIN_USER", "admin"))
    # Never hardcode the password: it comes from the environment (sourced from
    # docker/.env.dev — see SKILL.md). Keeps secrets out of the committed driver.
    ap.add_argument("--password", default=os.environ.get("LYNDRIX_ADMIN_PASSWORD"))
    ap.add_argument("--routes", nargs="*", default=DEFAULT_ROUTES)
    ap.add_argument("--outdir", default="shots")
    ap.add_argument("--no-mobile", action="store_true", help="desktop only")
    ap.add_argument("--health-only", action="store_true", help="curl /api/health, no browser")
    args = ap.parse_args()

    if not args.health_only and not args.password:
        print("error: set LYNDRIX_ADMIN_PASSWORD (e.g. from docker/.env.dev) or pass --password",
              file=sys.stderr)
        return 2

    health = http_health(args.base)
    print(f"core_version={health.get('core_version')} api_version={health.get('api_version')} "
          f"plugins={len(health.get('plugins', {}))}")
    if args.health_only:
        print(json.dumps(health, indent=2))
        return 0

    from playwright.sync_api import sync_playwright

    written = []
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        ctx = browser.new_context(viewport=DESKTOP)
        page = ctx.new_page()
        # Login page itself, before authenticating.
        written.append(shoot(page, args.base, "/login", DESKTOP, args.outdir, "desktop"))
        login(page, args.base, args.user, args.password)
        print("login: ok")
        for route in args.routes:
            written.append(shoot(page, args.base, route, DESKTOP, args.outdir, "desktop"))
            if not args.no_mobile:
                written.append(shoot(page, args.base, route, MOBILE, args.outdir, "mobile"))
        browser.close()

    for w in written:
        print(f"shot: {w}")
    print(f"wrote {len(written)} screenshots to {args.outdir}/")
    return 0


if __name__ == "__main__":
    sys.exit(main())
