import secrets
import json
from nicegui import ui
from ui.theme import UIStyles

def render_settings_ui(ctx, service):
    """Renders the settings interface for the IaC Orchestrator."""
    state = service.state
    current_config = {"auto_apply": state.get("auto_apply_enabled", False)}
    token_display = {"value": "********************************"}

    def save_settings():
        state["auto_apply_enabled"] = current_config["auto_apply"]
        ctx.set_secret("iac_auto_apply", str(current_config["auto_apply"]))
        ui.notify("Settings saved successfully.", type="positive")

    def generate_token():
        new_token = secrets.token_urlsafe(32)
        ctx.set_secret("gitlab_webhook_token", new_token)
        token_display["value"] = new_token
        ui.notify("New Webhook Token generated and stored in Vault.", type="positive")

    def get_token_registry():
        raw = ctx.get_secret("iac_token_registry")
        return json.loads(raw) if raw else []

    def save_token_registry(registry_list):
        ctx.set_secret("iac_token_registry", json.dumps(registry_list))

    token_registry = get_token_registry()
    token_options = {"": "None (Local or Public)"}
    for token in token_registry:
        token_options[token] = token

    token_dropdowns = []

    repo_roles = [
        {"slug": "iac_controller", "label": "IaC Controller (SSoT Source)"},
        {"slug": "infra_engine", "label": "Infrastructure Engine (Terraform/Tofu)"},
        {"slug": "config_engine", "label": "Configuration Engine (Ansible)"},
        {"slug": "inventory_state", "label": "Inventory State (Generated Output)"},
        {"slug": "aac_factory", "label": "AaC Factory (App Templates)"},
        {"slug": "service_repos", "label": "Application Services (Default Auth)"},
    ]

    def save_repo_config(slug, url, token_key):
        config = {"url": url, "token_key": token_key}
        ctx.set_secret(f"repo_{slug}_config", json.dumps(config))
        ui.notify(f"Configuration for {slug} saved.", type="positive")

    def load_repo_config(slug):
        raw = ctx.get_secret(f"repo_{slug}_config")
        if raw:
            try: return json.loads(raw)
            except Exception: pass
        return {"url": "", "token_key": ""}

    with ui.column().classes('w-full gap-6 pt-2'):

        # --- [SECTION 1: PIPELINE CONFIG] ---
        with ui.card().classes(f'{UIStyles.CARD_GLASS} w-full').style('padding: 0; flex-wrap: nowrap'):
            ui.element('div').classes('h-1 w-full bg-gradient-to-r from-indigo-400 via-sky-400 to-cyan-400')
            with ui.column().classes('w-full flex-grow p-5 gap-3'):
                with ui.row().classes('items-center gap-2 mb-1'):
                    ui.icon('tune', size='18px').classes('text-indigo-400')
                    ui.label('Pipeline Configuration').classes('text-sm font-bold uppercase tracking-widest text-slate-300')
                ui.switch('Enable Auto-Apply').bind_value(current_config, 'auto_apply').props('color=primary')
                ui.label('Warning: Auto-Apply executes infrastructure changes immediately on webhook receipt.').classes('text-xs text-orange-500 italic')
                with ui.row().classes('w-full justify-end mt-2'):
                    ui.button('Save Pipeline Settings', on_click=save_settings, icon='save', color='primary').props('unelevated rounded size=sm')

        # --- [SECTION 2: REPOSITORY ROLES] ---
        with ui.card().classes(f'{UIStyles.CARD_GLASS} w-full').style('padding: 0; flex-wrap: nowrap'):
            ui.element('div').classes('h-1 w-full bg-gradient-to-r from-emerald-400 via-teal-400 to-green-400')
            with ui.column().classes('w-full flex-grow p-5 gap-3'):
                with ui.row().classes('items-center gap-2 mb-1'):
                    ui.icon('folder_special', size='18px').classes('text-emerald-400')
                    ui.label('Repository Roles Configuration').classes('text-sm font-bold uppercase tracking-widest text-slate-300')
                ui.label('Map your backend Git repositories to functional orchestrator roles.').classes(UIStyles.TEXT_MUTED)

                for role in repo_roles:
                    current_repo_state = load_repo_config(role['slug'])
                    with ui.expansion(role['label'], icon='folder').classes('w-full border border-zinc-700 bg-zinc-900'):
                        with ui.column().classes('p-4 w-full gap-2'):
                            url_input = ui.input('Git Repository URL', value=current_repo_state.get('url', '')).classes('w-full').props('outlined dense')
                            token_select = ui.select(options=token_options, value=current_repo_state.get('token_key', ''), label='Vault Credential').classes('w-full').props('outlined dense')
                            token_dropdowns.append(token_select)

                            def trigger_test(slug=role['slug'], url=url_input, t_key=token_select):
                                target_url, vault_key = url.value, t_key.value
                                secret_value = ctx.get_secret(vault_key) if vault_key else ""
                                auth_type = "ssh" if target_url and ("git@" in target_url or "ssh" in target_url) else "token"
                                ctx.emit("git:sync", {"repo_id": slug, "url": target_url, "auth_type": auth_type, "secret_value": secret_value})
                                ui.notify(f"Sync command sent for {slug}. Check logs.", type="info")

                            with ui.row().classes('w-full justify-end mt-2 gap-4'):
                                ui.button('Test Sync', on_click=trigger_test, icon='sync', color='warning').props('unelevated rounded size=sm outline')
                                ui.button('Save Role', on_click=lambda r=role, u=url_input, t=token_select: save_repo_config(r['slug'], u.value, t.value), icon='save', color='secondary').props('unelevated rounded size=sm')

        # --- [SECTION 3: NATIVE ANSIBLE CONFIG] ---
        with ui.card().classes(f'{UIStyles.CARD_GLASS} w-full').style('padding: 0; flex-wrap: nowrap'):
            ui.element('div').classes('h-1 w-full bg-gradient-to-r from-amber-400 via-orange-400 to-yellow-500')
            with ui.column().classes('w-full flex-grow p-5 gap-3'):
                with ui.row().classes('items-center gap-2 mb-1'):
                    ui.icon('terminal', size='18px').classes('text-amber-400')
                    ui.label('Ansible Docker Configuration').classes('text-sm font-bold uppercase tracking-widest text-slate-300')
                ui.label('Configure the ephemeral Docker container and Registry Auth for Ansible Playbooks.').classes(UIStyles.TEXT_MUTED)

                default_img = "registry.gitlab.int.fam-feser.de/iac-environment/iac-platform-assets/ansible-ci-image:latest"
                img_input = ui.input('Docker Image', value=ctx.get_secret("ansible_docker_image") or default_img).props('outlined dense').classes('w-full')

                key_exists = bool(ctx.get_secret("ansible_ssh_key"))
                key_input = ui.textarea('Ansible SSH Private Key (RSA)', value="********************************\n(Key is set. Overwrite to change)" if key_exists else "").props('outlined dense').classes('w-full')

                ui.separator().classes('w-full my-2 opacity-50')
                ui.label('Private Registry Authentication (Optional)').classes('text-sm font-bold text-slate-200')

                reg_url_val = ctx.get_secret("ansible_registry_url") or ""
                reg_user_val = ctx.get_secret("ansible_registry_user") or ""
                reg_token_exists = bool(ctx.get_secret("ansible_registry_token"))

                reg_url_input = ui.input('Registry URL', value=reg_url_val).props('outlined dense').classes('w-full')
                reg_user_input = ui.input('Registry Username', value=reg_user_val).props('outlined dense').classes('w-full')
                reg_token_input = ui.input('Registry Token/Password', password=True, value="********" if reg_token_exists else "").props('outlined dense').classes('w-full')

                def save_ansible_config(img, key, r_url, r_user, r_token):
                    ctx.set_secret("ansible_docker_image", img.strip())
                    if key and "********" not in key: ctx.set_secret("ansible_ssh_key", key.strip())
                    ctx.set_secret("ansible_registry_url", r_url.strip())
                    ctx.set_secret("ansible_registry_user", r_user.strip())
                    if r_token and "********" not in r_token: ctx.set_secret("ansible_registry_token", r_token.strip())
                    ui.notify("Ansible and Registry Configuration saved to Vault.", type="positive")

                with ui.row().classes('w-full justify-end mt-2'):
                    ui.button('Save Ansible Config', on_click=lambda: save_ansible_config(img_input.value, key_input.value, reg_url_input.value, reg_user_input.value, reg_token_input.value), icon='terminal', color='indigo').props('unelevated rounded size=sm')

        # --- [SECTION 4: SECURITY CONFIG] ---
        with ui.card().classes(f'{UIStyles.CARD_GLASS} w-full').style('padding: 0; flex-wrap: nowrap'):
            ui.element('div').classes('h-1 w-full bg-gradient-to-r from-rose-400 via-pink-400 to-red-400')
            with ui.column().classes('w-full flex-grow p-5 gap-3'):
                with ui.row().classes('items-center gap-2 mb-1'):
                    ui.icon('security', size='18px').classes('text-rose-400')
                    ui.label('Security Configuration').classes('text-sm font-bold uppercase tracking-widest text-slate-300')

                ui.label('Webhook Authentication').classes(UIStyles.TEXT_MUTED)
                with ui.row().classes('w-full items-center gap-4'):
                    webhook_input = ui.input('GitLab Webhook Token').props('readonly outlined dense').classes('flex-1').bind_value(token_display, 'value')
                    ui.button('Generate Token', on_click=generate_token, icon='key', color='warning').props('unelevated rounded size=sm')

                ui.separator().classes('w-full my-2 opacity-30')

                ui.label('Git Credential Manager').classes(UIStyles.TEXT_MUTED)
                with ui.row().classes('w-full items-center gap-4'):
                    alias_input = ui.input('Credential Name (e.g., gitlab_main)').props('outlined dense').classes('flex-1')
                    secret_input = ui.input('Token or Private Key', password=True).props('outlined dense').classes('flex-1')

                    def add_new_credential():
                        alias, secret_val = alias_input.value.strip(), secret_input.value.strip()
                        if not alias or not secret_val: return ui.notify("Both Name and Secret are required.", type="negative")
                        ctx.set_secret(alias, secret_val)
                        registry = get_token_registry()
                        if alias not in registry:
                            registry.append(alias)
                            save_token_registry(registry)
                        token_options[alias] = alias
                        for dropdown in token_dropdowns: dropdown.update()
                        alias_input.value, secret_input.value = "", ""
                        ui.notify(f"Credential '{alias}' securely stored.", type="positive")

                    ui.button('Save Credential', on_click=add_new_credential, icon='lock', color='emerald').props('unelevated rounded size=sm')