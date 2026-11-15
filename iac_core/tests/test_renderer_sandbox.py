"""OS-05/OS-06: the generator renders the iac-controller's Jinja strings in a
sandbox — ordinary templates exactly as before, the walk to the interpreter not
at all."""
import sys
from pathlib import Path

from jinja2 import Environment
from jinja2.sandbox import SandboxedEnvironment

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "app"))

from services import renderer  # noqa: E402

CONFIG = {
    "global_vars": {"domain": "example.invalid", "ntp": ["10.0.0.1", "10.0.0.2"], "port": 8443,
                    "vault_vars": {"root_pub_key": "ssh-ed25519 AAAA dummy-not-real"}},
    "site_vars": {"site": "onprem", "dns_zone": "{{ site }}.{{ domain }}"},
    "stage_vars": {"stage": "prod"},
    "hosts": {
        "docker-a": {
            "fqdn": "docker-a.{{ dns_zone }}",                                   # needs the second pass
            "url": "https://{{ stage }}.traefik.docker-a.{{ domain }}:{{ port }}",
            "ntp": "{{ ntp | join(',') }}",
            "upper": "{{ site | upper }}-{{ stage | default('dev') }}",
            "missing": "x{{ not_defined_anywhere }}y",                           # PermissiveUndefined → ''
            "nested": "{{ global_vars.vault_vars.root_pub_key }}",
            "toplevel": "{{ toplevel_vars.site_vars.site }}",
            "cond": "{{ 'yes' if port > 1000 else 'no' }}",
            "math": "{{ port + 1 }}",
            "deploy_time": "{{ ansible_host_ip }}",                              # must survive as literal Jinja
            "items": ["{{ site }}", 5, {"deep": "{{ stage }}"}],
            "plain": "no template here",
        },
    },
}


def test_ordinary_templates_render_exactly_as_with_the_plain_environment(monkeypatch):
    sandboxed = renderer.render_templates(CONFIG)
    assert isinstance(_environment_used(monkeypatch), SandboxedEnvironment)
    monkeypatch.setattr(renderer, "SandboxedEnvironment", Environment)          # the renderer of <= 1.6.1
    plain = renderer.render_templates(CONFIG)
    assert sandboxed == plain
    host = sandboxed["hosts"]["docker-a"]
    assert host["fqdn"] == "docker-a.onprem.example.invalid" and host["ntp"] == "10.0.0.1,10.0.0.2"
    assert host["upper"] == "ONPREM-prod" and host["missing"] == "xy" and host["math"] == "8444"
    assert host["deploy_time"] == "{{ ansible_host_ip }}" and host["items"] == ["onprem", 5, {"deep": "prod"}]


def _environment_used(monkeypatch):
    seen = []
    real = renderer._render_pass
    monkeypatch.setattr(renderer, "_render_pass", lambda data, ctx, env: seen.append(env) or real(data, ctx, env))
    renderer.render_templates({"a": "{{ 1 }}"})
    monkeypatch.setattr(renderer, "_render_pass", real)
    return seen[0]


def test_a_template_cannot_reach_the_interpreter():
    """With a plain Environment each of these runs code in the generator process
    (which the core starts next to its secrets)."""
    escapes = {
        "subclasses": "{{ ''.__class__.__mro__[1].__subclasses__() }}",
        "globals": "{{ cycler.__init__.__globals__.os.environ }}",
        "builtins": "{{ self.__init__.__globals__.__builtins__ }}",
        "attr_filter": "{{ ''|attr('__class__') }}",
    }
    out = renderer.render_templates({"site_vars": {"site": "onprem"}, "evil": dict(escapes), "ok": "{{ site }}"})
    for name, template in escapes.items():
        rendered = out["evil"][name]
        # refused: left as it is (like any template that cannot be rendered) or empty — never evaluated
        assert rendered in (template, ""), name
        assert "class '" not in rendered and "environ(" not in rendered and "module" not in rendered
    assert out["ok"] == "onprem"                                                 # the rest still renders
