import os
import yaml
import asyncio
from pathlib import Path
from nicegui import ui
from ui.layout import main_layout 
from ui.theme import UIStyles

from .overview_dashboard import render_overview_dashboard
from .terraform import render_terraform_panel
from . import components as c
from ..controller.pipeline_meta import classify, describe

DOCKER_ICON = 'svg:M6.1,10L0,10.1V13h6.1V10z M13.1,10H7v3h6.1V10z M20.1,10H14v3h6.1V10z M13.1,3H7v3h6.1V3z'

async def render_dashboard(ctx, service):
    state = service.state
    engine = service.engine
    config = service.config
    active_job_cards = {} 

    def load_catalog():
        catalog_path = config.git_repos_dir / "iac_controller" / "environments" / "global" / "02_service_catalog.yml"
        if catalog_path.exists():
            try:
                with open(catalog_path, 'r') as f:
                    data = yaml.safe_load(f) or {}
                    return data.get("service_catalog", {}).get("services", [])
            except Exception as e: ctx.log.error(f"UI: Failed to parse catalog: {e}")
        return []

    def load_assignments():
        assignments = []
        base_dir = config.git_repos_dir / "iac_controller" / "environments"
        sites_dir = base_dir / "sites"
        profiles_file = base_dir / "global" / "03_profiles.yml"
        
        # 1. Load Profiles
        profiles = {}
        if profiles_file.exists():
            try:
                with open(profiles_file, 'r') as f:
                    p_data = yaml.safe_load(f) or {}
                    profiles = p_data.get("profiles") or {}
            except Exception as e:
                ctx.log.error(f"UI: Failed to parse profiles YAML: {e}")

        if not sites_dir.exists(): return []

        # 2. Parse Hosts
        for yaml_file in sites_dir.rglob("*.yml"):
            parts = yaml_file.parts
            try:
                site = parts[parts.index("sites") + 1]
                stage = parts[parts.index("stages") + 1] if "stages" in parts else "common"
                
                with open(yaml_file, 'r') as f:
                    data = yaml.safe_load(f) or {}
                    
                    hosts_data = data.get("hosts") or {}
                    hw_hosts_data = data.get("hardware_hosts") or {}
                    all_hosts = {**hosts_data, **hw_hosts_data}
                    
                    for host_name, host_data in all_hosts.items():
                        if not isinstance(host_data, dict): continue
                        
                        host_svcs = set()
                        
                        # Parse direct services
                        direct_services = host_data.get("services") or []
                        if isinstance(direct_services, list):
                            for s in direct_services:
                                if isinstance(s, dict) and s.get("name"): host_svcs.add(s.get("name"))
                                
                        # Parse profile-inherited services
                        host_profiles = host_data.get("profiles") or []
                        if isinstance(host_profiles, list):
                            for p in host_profiles:
                                profile_services = profiles.get(p, {}).get("services") or []
                                if isinstance(profile_services, list):
                                    for s in profile_services:
                                        if isinstance(s, dict) and s.get("name"): host_svcs.add(s.get("name"))
                                    
                        if host_svcs:
                            assignments.append({"site": site, "stage": stage, "host": host_name, "services": sorted(list(host_svcs))})
                            
            except (ValueError, IndexError):
                continue
            except Exception as e:
                ctx.log.error(f"UI: Failed to parse assignment YAML {yaml_file}: {e}")
                
        # Deduplicate and sort by site > stage > host
        unique_assignments = {f"{a['site']}-{a['stage']}-{a['host']}": a for a in assignments}
        return sorted(unique_assignments.values(), key=lambda x: (x['site'], x['stage'], x['host']))

    async def abort_execution():
        ctx.log.warning("UI: ABORT SEQUENCE INITIATED BY USER.")
        ui.notify("Aborting execution and destroying runner containers...", type="negative")
        for task_name in state.get("active_tasks", {}).keys():
            safe_task_name = "".join(c if c.isalnum() or c in ".-_" else "-" for c in task_name).strip("-")
            try: await asyncio.create_subprocess_exec("docker", "rm", "-f", f"aac-runner-{safe_task_name}")
            except Exception: pass
            
        recent = engine.db.get_recent_jobs(1)
        if recent and recent[0]["status"] == "RUNNING":
            job_id = recent[0]["id"]
            engine.db.update_job(job_id, "ABORTED")
            engine.db.update_progress(job_id, progress=None, current_step="Aborted by User")
            from core.api import OutboundMessage, MessageSeverity
            _abort_msg = OutboundMessage(
                title=f"Pipeline #{job_id} Aborted",
                body="Execution aborted by user.",
                severity=MessageSeverity.WARNING,
                source_plugin_id="lyndrix.plugin.iac_orchestrator",
                target_provider="system",
                metadata={"notification_id": f"job_{job_id}", "toast": True, "persist": True},
            )
            ctx.emit("messaging:outbound", _abort_msg.model_dump(mode="json"))
            
        state["is_running"] = False
        state["active_tasks"] = {}
        ui.notify("Execution Aborted Successfully.", type="info")
        
    # Live viewer state. ``offset`` is the byte position we have already streamed,
    # so each poll reads ONLY the new bytes (append-only log) instead of re-reading
    # and re-rendering the whole file. ``grep`` tracks the active filter term.
    active_log_job = {"id": None, "offset": 0, "grep": ""}
    # How much trailing log to seed the viewer with when it opens (recent context
    # without loading a huge file), and how far back grep scans for matches.
    LOG_SEED_BYTES = 200_000
    LOG_GREP_SCAN_BYTES = 1_000_000

    with ui.dialog() as log_viewer, ui.card().classes(f'w-full max-w-5xl h-[90vh] sm:h-[80vh] p-0 flex flex-col no-wrap !bg-black {UIStyles.MODAL_CONTAINER}'):
        with ui.row().classes('w-full p-3 sm:p-4 justify-between items-center gap-3 flex-wrap border-b border-zinc-800 bg-zinc-900'):
            with ui.row().classes('items-center gap-3 flex-wrap flex-1 min-w-0'):
                log_title = ui.label("Live Stream").classes('text-indigo-400 font-bold shrink-0')
                log_search = ui.input('Filter logs (grep)...').props('outlined dense clearable dark').classes('w-full sm:w-64')
            with ui.row().classes('items-center gap-1 shrink-0'):
                ui.button(icon='download', on_click=lambda: download_full_log()).props('flat round dense color=zinc-500').tooltip('Download full log')
                ui.button(icon='close', on_click=log_viewer.close).props('flat round dense color=zinc-500')
        # ui.log is an append-only, DOM-bounded element (older lines past max_lines are
        # dropped from the DOM) with built-in autoscroll — the right tool for streaming.
        log_stream = ui.log(max_lines=4000).classes('w-full flex-grow min-h-0 bg-black font-mono text-[11px] text-green-500')

    def download_full_log():
        jid = active_log_job["id"]
        if not jid:
            return
        p = config.get_log_path(jid)
        if p.exists():
            ui.download(str(p), filename=f"job_{jid}.log")
        else:
            ui.notify("No log file on disk for this job.", type='warning')

    def _push_lines(lines):
        for ln in lines:
            log_stream.push(ln)

    def update_log_content():
        if not log_viewer.value or not active_log_job["id"]:
            return
        log_path = config.get_log_path(active_log_job["id"])
        if not log_path.exists():
            return
        term = (log_search.value or "").lower()

        # Grep mode: re-render a bounded, filtered tail (scans only the last ~1MB).
        # Full-history search is available via the downloadable raw log.
        if term:
            try:
                size = log_path.stat().st_size
                with open(log_path, 'r', encoding='utf-8', errors='replace') as f:
                    f.seek(max(0, size - LOG_GREP_SCAN_BYTES))
                    chunk = f.read()
            except Exception:
                return
            matched = [ln for ln in chunk.split('\n') if term in ln.lower()]
            log_stream.clear()
            _push_lines(matched[-4000:])
            active_log_job["grep"] = term
            return

        # Just left grep mode -> reset back to incremental tailing from scratch.
        if active_log_job["grep"]:
            active_log_job["grep"] = ""
            active_log_job["offset"] = 0
            log_stream.clear()

        # Incremental mode: read only bytes appended since the stored offset.
        try:
            size = log_path.stat().st_size
            off = active_log_job["offset"]
            if off and off == size:
                return  # nothing new
            reset = False
            drop_partial = False
            if off == 0 or off > size:  # seed, or file shrank/rotated -> re-seed
                start = max(0, size - LOG_SEED_BYTES)
                drop_partial = start > 0
                reset = True
            else:
                start = off
            with open(log_path, 'r', encoding='utf-8', errors='replace') as f:
                f.seek(start)
                chunk = f.read()
            active_log_job["offset"] = size
        except Exception:
            return

        lines = chunk.split('\n')
        if lines and lines[-1] == '':
            lines.pop()
        if drop_partial and lines:
            lines = lines[1:]
        if reset:
            log_stream.clear()
        if lines:
            _push_lines(lines)

    log_search.on('update:model-value', update_log_content)
    ui.timer(1.0, update_log_content)

    def open_live_logs(job_id):
        active_log_job["id"] = job_id
        active_log_job["offset"] = 0
        active_log_job["grep"] = ""
        log_search.value = ""
        log_title.set_text(f"Live Pipeline Logs: Job #{job_id}")
        log_stream.clear()
        log_viewer.open()
        update_log_content()

    with ui.column().classes('w-full gap-6'):
        with ui.row().classes('w-full items-center justify-end'):
            ui.button('ABORT', on_click=abort_execution, icon='dangerous', color='red-6').props('unelevated rounded size=sm').bind_visibility_from(state, 'is_running')

        with ui.tabs().classes(UIStyles.TAB_BAR) as tabs:
            overview_tab = ui.tab('Overview', icon='dashboard')
            provision_tab = ui.tab('Provision', icon='dns')
            catalog_tab = ui.tab('Service Catalog', icon='apps')
            assignment_tab = ui.tab('Assignments', icon='account_tree')
            history_tab = ui.tab('History & Logs', icon='history')

        with ui.tab_panels(tabs, value=overview_tab).classes('w-full bg-transparent p-0'):
            
            with ui.tab_panel(overview_tab).classes('gap-6 p-4'):
                # Modern statistics dashboard (deployments, success rate, lifecycle phases)
                refresh_overview = render_overview_dashboard(ctx, service)
                # Keep KPIs/feed fresh as jobs progress and complete.
                ui.timer(8.0, refresh_overview)

                ui.separator().classes('opacity-10 my-2')

                ui.label("Active Pipelines").classes(UIStyles.TITLE_H3).bind_visibility_from(state, 'is_running')
                jobs_grid = ui.grid(columns='repeat(auto-fill, minmax(450px, 1fr))').classes('w-full gap-4')
                
                with ui.column().classes('w-full items-center py-16 opacity-30').bind_visibility_from(state, 'is_running', backward=lambda x: not x):
                    ui.icon('cloud_done', size='4em')
                    ui.label("Infrastructure is stable. No active jobs.").classes('text-lg font-bold')

            with ui.tab_panel(provision_tab).classes('gap-4 p-4'):
                render_terraform_panel(ctx, service)

            with ui.tab_panel(catalog_tab).classes('gap-4 p-4'):
                with ui.dialog() as svc_history_dialog, ui.card().classes(f'w-full max-w-4xl p-0 overflow-hidden {UIStyles.MODAL_CONTAINER} lyndrix-card'):
                    with ui.row().classes('w-full justify-between items-center p-4 border-b border-slate-200 dark:border-zinc-800 bg-slate-50/50 dark:bg-zinc-900/50'):
                        with ui.row().classes('items-center gap-3'):
                            ui.icon('history', size='24px').classes('text-primary')
                            svc_history_title = ui.label("").classes('text-lg font-bold text-slate-800 dark:text-zinc-100')
                        ui.button(icon='close', on_click=svc_history_dialog.close).props('flat round dense').classes('text-slate-500 hover:text-slate-800 dark:hover:text-white transition-colors')
                    
                    with ui.scroll_area().classes('w-full max-h-[60vh]'):
                        svc_history_table = ui.table(columns=[
                            {'name': 'id', 'label': 'Job ID', 'field': 'id', 'align': 'left'},
                            {'name': 'start_time', 'label': 'Date', 'field': 'start_time', 'align': 'left'},
                            {'name': 'status', 'label': 'Status', 'field': 'status', 'align': 'left'},
                            {'name': 'action', 'label': 'Log', 'field': 'action', 'align': 'center'}
                        ], rows=[], row_key='id').classes('w-full !bg-transparent shadow-none text-slate-800 dark:text-zinc-200').props('flat')
                        svc_history_table.add_slot('body-cell-status', '''<q-td :props="props"><q-badge :color="props.value === 'SUCCESS' ? 'positive' : (props.value === 'RUNNING' ? 'warning' : 'negative')">{{props.value}}</q-badge></q-td>''')
                        svc_history_table.add_slot('body-cell-action', '''<q-td :props="props"><q-btn flat round size="sm" icon="article" color="primary" @click="() => $parent.$emit('view', props.row)" /></q-td>''')
                        svc_history_table.on('view', lambda e: show_job_logs_wrapper(e.args['id']))

                def show_job_logs_wrapper(jid):
                    svc_history_dialog.close()
                    open_live_logs(jid)

                with ui.row().classes('w-full justify-between items-end mb-4'):
                    with ui.column().classes('gap-0'):
                        ui.label('Service Catalog').classes(UIStyles.TITLE_H3)
                        ui.label('Available services from the global catalog.').classes(f'{UIStyles.TEXT_MUTED} text-xs')
                    with ui.row().classes('gap-2 items-center'):
                        catalog_search = ui.input('Search Service...').props('outlined dense clearable').classes('w-64')
                        ui.button(icon='refresh', on_click=lambda: catalog_container.refresh()).props('flat round color=zinc-500')

                @ui.refreshable
                def catalog_container():
                    catalog_services = load_catalog()
                    if not catalog_services:
                        ui.label("No services found. Ensure 'iac_controller' is synced and YAML is valid.").classes(f'{UIStyles.TEXT_MUTED} italic mt-4')
                        return
                    
                    catalog_grid = ui.grid(columns='repeat(auto-fill, minmax(320px, 1fr))').classes('w-full gap-4 mt-2')
                    
                    def render_catalog_cards(e=None):
                        catalog_grid.clear()
                        term = (catalog_search.value or "").lower()
                        with catalog_grid:
                            for svc in catalog_services:
                                name = svc.get("name", "Unknown")
                                repo_name = svc.get("repository_name", name)
                                branch = svc.get("branch", "main")
                                target_node = svc.get("target_environment", svc.get("host", "Auto-Assigned"))
                                deploy_type = svc.get("deploy_type", "Docker Compose")
                                
                                match = not term or term in name.lower() or term in repo_name.lower() or term in target_node.lower()
                                
                                if match:
                                    with ui.card().classes(f'{UIStyles.CARD_BASE} flex flex-col hover:border-indigo-500 transition-colors').style('padding: 0; flex-wrap: nowrap'):
                                        ui.element('div').classes('h-1 w-full bg-gradient-to-r from-sky-400 via-cyan-400 to-indigo-400')
                                        with ui.column().classes('w-full flex-grow p-4 gap-2'):
                                            with ui.row().classes('w-full justify-between items-start'):
                                                with ui.column().classes('gap-0'):
                                                    ui.label(name).classes('text-md font-bold truncate')
                                                    ui.label(f"Repo: {repo_name}").classes(f'{UIStyles.TEXT_MUTED} text-[10px] truncate')

                                                if "compose" in deploy_type.lower():
                                                    ui.html('<svg viewBox="0 0 24 24" width="24" height="24" fill="currentColor"><path d="M6.1,10L0,10.1V13h6.1V10z M13.1,10H7v3h6.1V10z M20.1,10H14v3h6.1V10z M13.1,3H7v3h6.1V3z"/></svg>').classes('text-indigo-400 w-6 h-6').tooltip("Docker Compose")
                                                else:
                                                    ui.icon('settings_applications', color='slate-400').classes('text-xl').tooltip(deploy_type)

                                            ui.separator().classes('my-2 opacity-20')

                                            with ui.row().classes('w-full justify-between items-center'):
                                                with ui.row().classes('items-center gap-1'):
                                                    ui.icon('dns', size='12px').classes('text-slate-400')
                                                    ui.label(target_node).classes('text-xs text-slate-500 font-mono')

                                                with ui.row().classes('items-center gap-1'):
                                                    ui.icon('call_split', size='12px').classes('text-slate-400')
                                                    ui.label(branch).classes('text-xs text-slate-500 font-mono')

                                            ui.separator().classes('mt-auto mb-3 opacity-20')

                                            with ui.row().classes('w-full justify-between items-center gap-2'):
                                                ui.button(icon='history', on_click=lambda n=name: [svc_history_title.set_text(f"Deployment History: {n}"), setattr(svc_history_table, 'rows', engine.db.get_service_history(n)), svc_history_dialog.open()]).props('flat round size=sm color=zinc-500').tooltip("View Deployment History")
                                                ui.button('Deploy', icon='rocket', on_click=lambda n=name, b=branch: ctx.emit("iac:webhook_verified", {"pipeline_type": "single_service", "service_name": n, "service_branch": b, "manual": True})).props('unelevated rounded size=sm color=indigo')

                    catalog_search.on('update:model-value', render_catalog_cards)
                    render_catalog_cards()

                catalog_container()

            with ui.tab_panel(assignment_tab).classes('p-4'):
                with ui.row().classes('w-full justify-between items-end mb-4'):
                    with ui.column().classes('gap-0'):
                        ui.label('Infrastructure Topography').classes(UIStyles.TITLE_H3)
                        ui.label('Flattened view of mapped services across all sites and stages.').classes(f'{UIStyles.TEXT_MUTED} text-xs')
                    with ui.row().classes('gap-2 items-center'):
                        search_input = ui.input('Search Host or Service...').props('outlined dense clearable').classes('w-64')
                        ui.button(icon='refresh', on_click=lambda: assignment_container.refresh()).props('flat round color=zinc-500')
                        ui.button('Global Bootstrap', icon='verified_user', color='sky', on_click=lambda: ctx.emit("iac:webhook_verified", {"pipeline_type": "bootstrap_compliance", "limit": "all", "manual": True})).props('flat rounded size=sm').bind_enabled_from(state, 'is_running', backward=lambda x: not x).tooltip("Run compliance/baseline (as root) across ALL hosts")
                        ui.button('Global Adopt', icon='move_to_inbox', color='amber-7', on_click=lambda: ctx.emit("iac:webhook_verified", {"pipeline_type": "adopt_host", "limit": "all", "manual": True})).props('flat rounded size=sm').bind_enabled_from(state, 'is_running', backward=lambda x: not x).tooltip("Import every managed container (all sites) into Terraform state")
                        ui.button('Global Rollout', icon='public', color='emerald', on_click=lambda: ctx.emit("iac:webhook_verified", {"pipeline_type": "rollout", "limit": "all", "manual": True})).props('unelevated rounded size=sm').bind_enabled_from(state, 'is_running', backward=lambda x: not x).tooltip("Trigger full infrastructure rollout")

                @ui.refreshable
                def assignment_container():
                    assignments = load_assignments()
                    if not assignments:
                        ui.label("No assignments found. Ensure 'iac_controller/environments' is populated.").classes(f'{UIStyles.TEXT_MUTED} italic mt-4')
                        return

                    def render_cards(e=None):
                        term = (search_input.value or "").lower()
                        
                        # Group by Site and Stage
                        sites = {}
                        for item in assignments:
                            site, stage, host, svcs = item['site'], item['stage'], item['host'], item['services']
                            if not term or term in host.lower() or term in site.lower() or term in stage.lower() or any(term in s.lower() for s in svcs):
                                if site not in sites: sites[site] = {}
                                if stage not in sites[site]: sites[site][stage] = []
                                sites[site][stage].append(item)

                        assignment_wrapper.clear()
                        with assignment_wrapper:
                            if not sites:
                                ui.label("No matching hosts found.").classes(f'{UIStyles.TEXT_MUTED} italic mt-4')
                                return
                                
                            for site, stages in sorted(sites.items()):
                                with ui.column().classes('w-full mt-4 gap-2'):
                                    with ui.row().classes('w-full items-center gap-3 border-b border-zinc-800 pb-2'):
                                        ui.icon('domain', size='24px').classes('text-slate-400')
                                        ui.label(site.upper()).classes('text-xl font-black tracking-widest text-slate-800 dark:text-slate-200')
                                        ui.space()
                                        ui.button('Site Bootstrap', icon='verified_user', on_click=lambda s=site: ctx.emit("iac:webhook_verified", {"pipeline_type": "bootstrap_compliance", "limit": s, "manual": True})).props('flat rounded size=sm color=sky').bind_enabled_from(state, 'is_running', backward=lambda x: not x).tooltip(f"Run compliance/baseline (as root) across all {site.upper()} hosts")
                                        ui.button('Site Adopt', icon='move_to_inbox', on_click=lambda s=site: ctx.emit("iac:webhook_verified", {"pipeline_type": "adopt_host", "limit": s, "manual": True})).props('flat rounded size=sm color=amber-7').bind_enabled_from(state, 'is_running', backward=lambda x: not x).tooltip(f"Import all managed {site.upper()} containers into Terraform state")
                                        ui.button('Site Rollout', icon='rocket_launch', on_click=lambda s=site: ctx.emit("iac:webhook_verified", {"pipeline_type": "rollout", "limit": s, "manual": True})).props('flat rounded size=sm color=slate').bind_enabled_from(state, 'is_running', backward=lambda x: not x).tooltip(f"Rollout all hosts in {site.upper()}")

                                    for stage, items in sorted(stages.items()):
                                        with ui.column().classes('w-full pl-4 md:pl-6 border-l-2 border-zinc-800/50 mt-2 gap-3'):
                                            with ui.row().classes('items-center gap-2'):
                                                ui.icon('layers', size='16px').classes('text-emerald-500')
                                                ui.label(stage.upper()).classes('text-sm font-bold text-emerald-400 tracking-wider')
                                            
                                            with ui.grid(columns='repeat(auto-fill, minmax(350px, 1fr))').classes('w-full gap-4'):
                                                for item in items:
                                                    host, svcs = item['host'], item['services']
                                                    with ui.card().classes(f'{UIStyles.CARD_BASE} flex flex-col gap-2 hover:border-indigo-500/50 transition-all').style('padding: 0; flex-wrap: nowrap'):
                                                        ui.element('div').classes('h-1 w-full bg-gradient-to-r from-emerald-400 via-teal-400 to-green-400')
                                                        with ui.column().classes('w-full flex-grow p-4 gap-2'):
                                                            with ui.row().classes('w-full justify-between items-center border-b border-zinc-800/50 pb-2'):
                                                                with ui.row().classes('items-center gap-2'):
                                                                    ui.icon('dns', size='18px').classes('text-slate-400')
                                                                    ui.label(host).classes('text-md font-bold text-slate-800 dark:text-slate-300 truncate max-w-[150px]').tooltip(host)
                                                                with ui.row().classes('gap-1 items-center'):
                                                                    ui.button('Adopt Host', icon='move_to_inbox', on_click=lambda h=host: ctx.emit("iac:webhook_verified", {"pipeline_type": "adopt_host", "host_name": h, "manual": True})).props('unelevated rounded size=sm color=amber-7').tooltip(f"Import the existing container for {host} into Terraform state (import + plan, no apply)").bind_enabled_from(state, 'is_running', backward=lambda x: not x)
                                                                    ui.button('Init Host', icon='dns', on_click=lambda h=host: ctx.emit("iac:webhook_verified", {"pipeline_type": "init_host", "host_name": h, "manual": True})).props('unelevated rounded size=sm color=deep-purple').tooltip(f"Provision the container for {host} via Terraform only (no Ansible, no services)").bind_enabled_from(state, 'is_running', backward=lambda x: not x)
                                                                    ui.button('Bootstrap Host', icon='verified_user', on_click=lambda h=host: ctx.emit("iac:webhook_verified", {"pipeline_type": "bootstrap_compliance", "host_name": h, "manual": True})).props('unelevated rounded size=sm color=sky').tooltip(f"Run the initial compliance/baseline playbook as root on {host} (creates the ansible-agent account)").bind_enabled_from(state, 'is_running', backward=lambda x: not x)
                                                                    ui.button('Compliance Host', icon='fact_check', on_click=lambda h=host: ctx.emit("iac:webhook_verified", {"pipeline_type": "compliance", "host_name": h, "manual": True})).props('unelevated rounded size=sm color=teal').tooltip(f"Re-run the compliance baseline as the svc user (ansible-agent) on {host} — no service deployment").bind_enabled_from(state, 'is_running', backward=lambda x: not x)
                                                                    ui.button('Deploy Services', icon='rocket', on_click=lambda h=host: ctx.emit("iac:webhook_verified", {"pipeline_type": "rollout", "limit": h, "manual": True})).props('unelevated rounded size=sm color=indigo').tooltip(f"Deploy this host's services to {host}").bind_enabled_from(state, 'is_running', backward=lambda x: not x)

                                                            with ui.row().classes('gap-1.5 pt-1'):
                                                                for svc in svcs:
                                                                    ui.chip(svc, icon='apps', color='zinc-800').props('text-color=slate-300 size=sm')
                    
                    assignment_wrapper = ui.column().classes('w-full')
                    search_input.on('update:model-value', render_cards)
                    render_cards()
                    
                assignment_container()

            with ui.tab_panel(history_tab).classes('p-3 sm:p-4'):
                with ui.row().classes('w-full justify-between items-center mb-3 gap-2 flex-wrap'):
                    ui.label('Deployment History').classes(UIStyles.TITLE_H3)
                    history_search = ui.input('Search Job ID, Type or Status…') \
                        .props('outlined dense clearable') \
                        .classes('w-full sm:w-64')
                history_grid = ui.element('div').classes(
                    'grid grid-cols-1 md:grid-cols-2 xl:grid-cols-3 gap-3 w-full'
                )
        
        def _format_duration(start_str, end_str) -> str:
            if not start_str:
                return ''
            try:
                from datetime import datetime, timezone
                start = datetime.fromisoformat(str(start_str)[:19]).replace(tzinfo=timezone.utc)
                end   = datetime.fromisoformat(str(end_str)[:19]).replace(tzinfo=timezone.utc) \
                        if end_str else datetime.now(timezone.utc)
                secs = max(0, int((end - start).total_seconds()))
                if secs < 60:   return f'{secs}s'
                if secs < 3600: return f'{secs // 60}m {secs % 60}s'
                return f'{secs // 3600}h {(secs % 3600) // 60}m'
            except Exception:
                return ''

        def _render_job_card(job: dict) -> None:
            status   = job.get('status') or 'UNKNOWN'
            p_type   = job.get('pipeline_type') or 'unknown'
            pdef     = classify(p_type)
            progress = int(job.get('progress') or 0)

            strip_color = (
                'indigo'  if status == 'RUNNING'                        else
                'rose'    if status in ('FAILED', 'ERROR', 'ABORTED')   else
                'emerald' if status == 'SUCCESS'                         else
                pdef.color
            )
            duration_str = _format_duration(job.get('start_time'), job.get('end_time'))

            with c.tile(strip_color, inner='w-full p-3 gap-2', hover=False):
                # Row 1: type label + status badge
                with ui.row().classes('w-full items-center justify-between gap-2 flex-wrap'):
                    with ui.row().classes('items-center gap-2 min-w-0 flex-1'):
                        ui.icon(pdef.icon, size='16px').classes(c.accent_text(pdef.color))
                        ui.label(describe(p_type)).classes(
                            'text-sm font-bold text-slate-800 dark:text-zinc-100 truncate'
                        ).tooltip(describe(p_type))
                    c.status_badge(status)

                # Row 2: job ID · start time · duration
                with ui.row().classes('w-full items-center justify-between gap-1 flex-wrap'):
                    ui.label(f'#{job["id"]} · {job.get("start_time") or "—"}').classes(
                        UIStyles.TEXT_MUTED + ' text-xs font-mono'
                    )
                    if duration_str:
                        ui.label(f'in {duration_str}').classes(UIStyles.TEXT_MUTED + ' text-xs')

                # Row 3: progress bar + log button
                with ui.row().classes('w-full items-center gap-2'):
                    with ui.column().classes('flex-1 gap-0.5'):
                        c.progress_bar(progress, pdef.color)
                        ui.label(f'{progress}%').classes(UIStyles.LABEL_MINI)
                    ui.button(
                        icon='terminal',
                        on_click=lambda jid=job['id']: open_live_logs(jid),
                    ).props('flat round dense size=sm color=zinc-500').tooltip('View Logs')

                # Row 4: current step (only when present)
                step = (job.get('current_step') or '').strip()
                if step:
                    ui.label(step).classes(
                        UIStyles.TEXT_MUTED + ' text-[10px] font-mono truncate w-full'
                    )

        _history_hash: list = [None]

        def update_ui_loop():
            running_jobs = engine.db.get_jobs_by_status("RUNNING")
            active_ids = [j.id for j in running_jobs]
            
            for jid in list(active_job_cards.keys()):
                if jid not in active_ids:
                    jobs_grid.remove(active_job_cards[jid]["card"])
                    del active_job_cards[jid]

            with jobs_grid:
                for job in running_jobs:
                    if job.id not in active_job_cards:
                        with ui.card().classes(f'{UIStyles.CARD_GLASS} flex flex-col shadow-2xl').style('padding: 0; flex-wrap: nowrap') as c:
                            ui.element('div').classes('h-1 w-full bg-gradient-to-r from-indigo-400 via-violet-400 to-purple-400')
                            with ui.column().classes('w-full flex-grow p-4 gap-0'):
                                with ui.row().classes('w-full justify-between items-start'):
                                    with ui.column().classes('gap-0'):
                                        ui.label(f"Pipeline #{job.id}").classes('text-lg font-bold text-indigo-400')
                                        ui.label(job.pipeline_type).classes('text-[10px] uppercase text-slate-500 font-black tracking-widest')
                                    ui.spinner('tail', size='2em', color='indigo')

                                with ui.linear_progress(value=(job.progress or 0)/100.0, show_value=False).props('color=indigo rounded stripe size=20px').classes('mt-4 relative') as p_bar:
                                    pct_lbl = ui.label(f"{int(job.progress or 0)}%").classes('absolute-center text-[11px] font-bold text-white drop-shadow-md')
                                with ui.row().classes('w-full mt-1'):
                                    step_lbl = ui.label(job.current_step).classes('text-[11px] font-mono text-slate-300 truncate w-full')

                                ui.label("Active Runners").classes('text-[10px] uppercase text-zinc-600 font-bold mt-4 mb-1')
                                runner_box = ui.column().classes('w-full gap-1 p-2 bg-black/40 border border-zinc-800/50')

                                with ui.row().classes('w-full mt-4 pt-2 border-t border-zinc-800 justify-between'):
                                    ui.button('Live Logs', icon='terminal', on_click=lambda j=job.id: open_live_logs(j)).props('flat rounded size=sm color=green')
                                    ui.button('Abort', icon='stop', on_click=abort_execution).props('flat rounded size=sm color=red')
                            
                        active_job_cards[job.id] = {"card": c, "bar": p_bar, "step": step_lbl, "pct": pct_lbl, "runners": runner_box}
                    else:
                        card_meta = active_job_cards[job.id]
                        card_meta["bar"].set_value((job.progress or 0) / 100.0)
                        card_meta["step"].set_text(job.current_step)
                        card_meta["pct"].set_text(f"{int(job.progress or 0)}%")
                        
                        card_meta["runners"].clear()
                        any_runners = False
                        for t_name, t_data in state.get("active_tasks", {}).items():
                            if t_data.get("job_id") == job.id and t_data.get("status") in ["pulling_image", "running_ansible"]:
                                any_runners = True
                                with card_meta["runners"]:
                                    with ui.row().classes('w-full items-center gap-2 px-1'):
                                        ui.icon('settings_input_component', size='12px', color='amber-500')
                                        ui.label(t_name).classes('text-[10px] text-slate-300 font-medium truncate w-4/5')
                                        ui.spinner('dots', size='xs', color='slate-600').classes('ml-auto')
                        
                        if not any_runners:
                            with card_meta["runners"]:
                                ui.label("Waiting for pool...").classes('text-[10px] text-zinc-600 italic px-1')

            if tabs.value == 'History & Logs':
                term     = (history_search.value or '').lower()
                all_jobs = engine.db.get_recent_jobs(100 if term else 30)
                filtered = [
                    j for j in all_jobs
                    if term in str(j['id']) or term in str(j['pipeline_type']).lower()
                    or term in str(j['status']).lower()
                ] if term else all_jobs

                new_hash = hash(str([
                    (j['id'], j['status'], j.get('progress'), j.get('current_step'))
                    for j in filtered
                ]))
                if new_hash != _history_hash[0]:
                    _history_hash[0] = new_hash
                    history_grid.clear()
                    with history_grid:
                        if filtered:
                            for job in filtered:
                                _render_job_card(job)
                        else:
                            with ui.column().classes(
                                'col-span-full w-full items-center py-12 gap-2 opacity-40'
                            ):
                                ui.icon('history', size='3em')
                                ui.label('No deployments found.').classes(UIStyles.TEXT_MUTED)

        ui.timer(1.0, update_ui_loop)