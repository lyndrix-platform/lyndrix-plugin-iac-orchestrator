import secrets
import json
import asyncio
from nicegui import ui
from ui.theme import UIStyles
from ..controller.gitlab_webhooks import upsert_gitlab_group_webhooks

def render_settings_ui(ctx, service):
    """Renders the settings interface for the IaC Orchestrator."""
    state = service.state
    current_config = {
        "auto_apply": state.get("auto_apply_enabled", False),
        "test_deploy_allowed_hosts": ctx.get_secret("iac_test_deploy_allowed_hosts") or "",
    }
    gitlab_webhook_config = {
        "gitlab_url": ctx.get_secret("iac_gitlab_url") or "https://gitlab.int.fam-feser.de",
        "group_id": ctx.get_secret("iac_gitlab_group_id") or "",
        "lyndrix_base_url": ctx.get_secret("iac_lyndrix_base_url") or "http://10.1.10.31:8081",
        "gitlab_token_key": ctx.get_secret("iac_gitlab_api_token_key") or "",
    }
    token_display = {"value": "********************************"}

    def save_settings():
        state["auto_apply_enabled"] = current_config["auto_apply"]
        ctx.set_secret("iac_auto_apply", str(current_config["auto_apply"]))
        ctx.set_secret(
            "iac_test_deploy_allowed_hosts",
            str(current_config["test_deploy_allowed_hosts"] or "").strip(),
        )
        ui.notify("Settings saved successfully.", type="positive")

    def save_gitlab_webhook_settings():
        ctx.set_secret("iac_gitlab_url", str(gitlab_webhook_config["gitlab_url"] or "").strip())
        ctx.set_secret("iac_gitlab_group_id", str(gitlab_webhook_config["group_id"] or "").strip())
        ctx.set_secret("iac_lyndrix_base_url", str(gitlab_webhook_config["lyndrix_base_url"] or "").strip())
        ctx.set_secret("iac_gitlab_api_token_key", str(gitlab_webhook_config["gitlab_token_key"] or "").strip())
        ui.notify("GitLab webhook settings saved.", type="positive")

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
                ui.input(
                    'Test Deploy Allowed Hosts (comma-separated)',
                    placeholder='e.g. pve-test-01',
                ).props('outlined dense').classes('w-full').bind_value(
                    current_config, 'test_deploy_allowed_hosts'
                )
                ui.label(
                    'Used by /api/iac/deploy/test-host/{host}; blocks rollout to non-allowlisted hosts.'
                ).classes('text-xs text-slate-400')

                def _allowlisted_hosts() -> list:
                    raw = current_config.get('test_deploy_allowed_hosts') or ''
                    if isinstance(raw, (list, tuple)):
                        items = list(raw)
                    else:
                        items = str(raw).replace('\n', ',').split(',')
                    return [h.strip() for h in items if h.strip()]

                with ui.dialog() as test_deploy_dialog, ui.card().classes(
                    f'{UIStyles.MODAL_CONTAINER} !bg-zinc-900 border border-violet-500/40'
                ):
                    with ui.column().classes('gap-3 p-2'):
                        with ui.row().classes('items-center gap-2'):
                            ui.icon('warning', size='22px').classes('text-violet-400')
                            ui.label('Run test deploy?').classes('text-base font-bold text-slate-100')
                        dialog_body = ui.label('').classes('text-xs text-slate-400 max-w-md')
                        with ui.row().classes('w-full justify-end gap-2 mt-1'):
                            ui.button('Cancel', on_click=test_deploy_dialog.close).props(
                                'flat rounded size=sm color=zinc'
                            )

                            def _run_test_deploy():
                                hosts = _allowlisted_hosts()
                                for hn in hosts:
                                    ctx.emit('iac:webhook_verified', {
                                        'pipeline_type': 'host_provision',
                                        'host_name': hn,
                                        'approve': True,
                                        'manual': True,
                                        'trigger': 'ui_settings_test_deploy',
                                    })
                                test_deploy_dialog.close()
                                ui.notify(
                                    f"Test deploy queued for: {', '.join(hosts)}",
                                    type='positive',
                                )

                            ui.button(
                                'Run Test Deploy', icon='rocket_launch', color='violet',
                                on_click=_run_test_deploy,
                            ).props('unelevated rounded size=sm')

                def _open_test_deploy():
                    hosts = _allowlisted_hosts()
                    if not hosts:
                        ui.notify(
                            'No allowed test hosts configured. Add one above and save first.',
                            type='warning',
                        )
                        return
                    dialog_body.set_text(
                        'This provisions and bootstraps the allowlisted test host(s) '
                        f"({', '.join(hosts)}) with Terraform + Ansible. Real infrastructure "
                        'will be created or changed.'
                    )
                    test_deploy_dialog.open()

                with ui.row().classes('w-full justify-end mt-2 gap-2'):
                    ui.button(
                        'Run Test Deploy', on_click=_open_test_deploy,
                        icon='rocket_launch', color='violet',
                    ).props('outline rounded size=sm').tooltip(
                        'Provision the allowlisted test host(s) (Terraform + Ansible bootstrap)'
                    )
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

        # --- [SECTION 3b: TERRAFORM PROVISIONING SECRETS] ---
        with ui.card().classes(f'{UIStyles.CARD_GLASS} w-full').style('padding: 0; flex-wrap: nowrap'):
            ui.element('div').classes('h-1 w-full bg-gradient-to-r from-purple-400 via-fuchsia-400 to-violet-500')
            with ui.column().classes('w-full flex-grow p-5 gap-3'):
                with ui.row().classes('items-center gap-2 mb-1'):
                    ui.icon('dns', size='18px').classes('text-purple-400')
                    ui.label('Terraform Provisioning Secrets').classes('text-sm font-bold uppercase tracking-widest text-slate-300')
                ui.label('Sensitive root credentials injected into provisioned hosts. Stored in Vault, never in the repo.').classes(UIStyles.TEXT_MUTED)

                tf_key_exists = bool(ctx.get_secret("iac_tf_ssh_key"))
                tf_key_input = ui.textarea('Root SSH Public Key (injected into new hosts)', value="********************************\n(Key is set. Overwrite to change)" if tf_key_exists else "").props('outlined dense').classes('w-full')

                tf_priv_key_exists = bool(ctx.get_secret("iac_tf_ssh_private_key"))
                tf_priv_key_input = ui.textarea('Root SSH Private Key (bootstrap / first compliance run)', value="********************************\n(Key is set. Overwrite to change)" if tf_priv_key_exists else "").props('outlined dense').classes('w-full')
                ui.label('Private counterpart of the public key above. Used to connect as root for the initial compliance/bootstrap run before the ansible-agent account exists.').classes('text-xs text-slate-400')

                tf_root_pw_exists = bool(ctx.get_secret("iac_tf_root_password"))
                tf_root_pw_input = ui.input('Root Password (new host)', password=True, value="********" if tf_root_pw_exists else "").props('outlined dense').classes('w-full')

                ui.label('These take precedence only when not set in terraform_vars; keeping them here keeps the repo free of private credentials.').classes('text-xs text-slate-400')

                def save_terraform_secrets(ssh_key, priv_key, root_pw):
                    if ssh_key and "********" not in ssh_key: ctx.set_secret("iac_tf_ssh_key", ssh_key.strip())
                    if priv_key and "********" not in priv_key: ctx.set_secret("iac_tf_ssh_private_key", priv_key.strip())
                    if root_pw and "********" not in root_pw: ctx.set_secret("iac_tf_root_password", root_pw.strip())
                    ui.notify("Terraform provisioning secrets saved to Vault.", type="positive")

                with ui.row().classes('w-full justify-end mt-2'):
                    ui.button('Save Terraform Secrets', on_click=lambda: save_terraform_secrets(tf_key_input.value, tf_priv_key_input.value, tf_root_pw_input.value), icon='dns', color='purple').props('unelevated rounded size=sm')

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

        # --- [SECTION 4b: GITLAB WEBHOOK MANAGEMENT] ---
        with ui.card().classes(f'{UIStyles.CARD_GLASS} w-full').style('padding: 0; flex-wrap: nowrap'):
            ui.element('div').classes('h-1 w-full bg-gradient-to-r from-sky-400 via-blue-400 to-indigo-500')
            with ui.column().classes('w-full flex-grow p-5 gap-3'):
                with ui.row().classes('items-center gap-2 mb-1'):
                    ui.icon('webhook', size='18px').classes('text-sky-400')
                    ui.label('GitLab Webhooks').classes('text-sm font-bold uppercase tracking-widest text-slate-300')
                ui.label(
                    'Upsert merge-request-only webhooks for all projects in a GitLab group '
                    'to the Lyndrix orchestrator endpoint.'
                ).classes(UIStyles.TEXT_MUTED)

                ui.input('GitLab Base URL').props('outlined dense').classes('w-full').bind_value(
                    gitlab_webhook_config, 'gitlab_url'
                )
                ui.input('GitLab Group ID').props('outlined dense').classes('w-full').bind_value(
                    gitlab_webhook_config, 'group_id'
                )
                ui.input('Lyndrix Base URL').props('outlined dense').classes('w-full').bind_value(
                    gitlab_webhook_config, 'lyndrix_base_url'
                )
                token_select_webhook = ui.select(
                    options=token_options,
                    value=gitlab_webhook_config.get('gitlab_token_key', ''),
                    label='GitLab API Credential (Vault key)',
                ).classes('w-full').props('outlined dense').bind_value(
                    gitlab_webhook_config, 'gitlab_token_key'
                )
                token_dropdowns.append(token_select_webhook)

                webhook_preview = ui.input(
                    'Webhook Endpoint Preview',
                    value=f"{str(gitlab_webhook_config.get('lyndrix_base_url') or '').rstrip('/')}/api/iac/webhook/gitlab",
                ).props('readonly outlined dense').classes('w-full')

                def _refresh_webhook_preview():
                    webhook_preview.value = (
                        f"{str(gitlab_webhook_config.get('lyndrix_base_url') or '').rstrip('/')}/api/iac/webhook/gitlab"
                    )
                    webhook_preview.update()

                async def run_webhook_upsert():
                    _refresh_webhook_preview()
                    save_gitlab_webhook_settings()

                    group_id_raw = str(gitlab_webhook_config.get('group_id') or '').strip()
                    if not group_id_raw.isdigit():
                        ui.notify("GitLab Group ID must be a number.", type="negative")
                        return

                    token_key = str(gitlab_webhook_config.get('gitlab_token_key') or '').strip()
                    if not token_key:
                        ui.notify("Select a GitLab API credential Vault key.", type="negative")
                        return
                    gitlab_token = ctx.get_secret(token_key)
                    if not gitlab_token:
                        ui.notify(f"Vault key '{token_key}' has no secret value.", type="negative")
                        return

                    webhook_token = ctx.get_secret("gitlab_webhook_token")
                    if not webhook_token:
                        ui.notify("Generate a GitLab webhook token first in Security Configuration.", type="negative")
                        return

                    ui.notify("Configuring GitLab webhooks…", type="info")
                    try:
                        result = await asyncio.to_thread(
                            upsert_gitlab_group_webhooks,
                            str(gitlab_webhook_config.get('gitlab_url') or '').strip(),
                            gitlab_token,
                            int(group_id_raw),
                            str(gitlab_webhook_config.get('lyndrix_base_url') or '').strip(),
                            webhook_token,
                        )
                    except Exception as exc:
                        ui.notify(f"Webhook upsert failed: {exc}", type="negative")
                        return

                    if result["failed"] > 0:
                        ui.notify(
                            f"Webhook upsert done with errors. "
                            f"Updated={result['updated']}, Created={result['created']}, Failed={result['failed']}",
                            type="warning",
                            timeout=10,
                        )
                        for err in result["errors"][:5]:
                            ui.notify(err, type="warning", timeout=8)
                    else:
                        ui.notify(
                            f"Webhook upsert successful. "
                            f"Projects={result['projects_total']}, Updated={result['updated']}, Created={result['created']}",
                            type="positive",
                        )

                with ui.row().classes('w-full justify-end mt-2 gap-2'):
                    ui.button(
                        'Save Webhook Settings',
                        on_click=save_gitlab_webhook_settings,
                        icon='save',
                        color='secondary',
                    ).props('unelevated rounded size=sm')
                    ui.button(
                        'Upsert GitLab Webhooks',
                        on_click=run_webhook_upsert,
                        icon='webhook',
                        color='primary',
                    ).props('unelevated rounded size=sm')

        # --- [SECTION 5: MAINTENANCE / DATA] ---
        def clear_stats():
            deleted = service.db.clear_all_jobs(keep_running=True)
            if deleted < 0:
                ui.notify("Failed to clear statistics (see logs).", type="negative")
                return
            service.state["last_deployment"] = "N/A"
            ui.notify(f"Cleared {deleted} job record(s). Statistics reset.", type="positive")

        with ui.dialog() as clear_stats_dialog, ui.card().classes('bg-zinc-900 border border-rose-500/40'):
            with ui.column().classes('gap-3 p-2'):
                with ui.row().classes('items-center gap-2'):
                    ui.icon('warning', size='22px').classes('text-rose-400')
                    ui.label('Clear all statistics?').classes('text-base font-bold text-slate-100')
                ui.label(
                    'This permanently deletes all deployment job history that feeds the '
                    'Overview KPIs and recent-deployments feed. Currently running jobs are '
                    'kept. This cannot be undone.'
                ).classes('text-xs text-slate-400 max-w-md')
                with ui.row().classes('w-full justify-end gap-2 mt-1'):
                    ui.button('Cancel', on_click=clear_stats_dialog.close).props('flat rounded size=sm color=zinc')
                    ui.button(
                        'Clear Stats', icon='delete_forever', color='negative',
                        on_click=lambda: [clear_stats(), clear_stats_dialog.close()],
                    ).props('unelevated rounded size=sm')

        with ui.card().classes(f'{UIStyles.CARD_GLASS} w-full').style('padding: 0; flex-wrap: nowrap'):
            ui.element('div').classes('h-1 w-full bg-gradient-to-r from-rose-500 via-red-500 to-orange-500')
            with ui.column().classes('w-full flex-grow p-5 gap-3'):
                with ui.row().classes('items-center gap-2 mb-1'):
                    ui.icon('cleaning_services', size='18px').classes('text-rose-400')
                    ui.label('Maintenance').classes('text-sm font-bold uppercase tracking-widest text-slate-300')

                with ui.row().classes('w-full items-center justify-between gap-4 flex-wrap'):
                    with ui.column().classes('gap-0.5'):
                        ui.label('Sync Core Repositories').classes('text-sm font-semibold text-zinc-200')
                        ui.label(
                            'Fetches the latest state from all four core Git repositories '
                            '(IaC Controller, Inventory State, Config Engine, AaC Factory). '
                            'Runs automatically on every deployment.'
                        ).classes(UIStyles.TEXT_MUTED + ' text-xs max-w-lg')
                    ui.button(
                        'Resync Repositories', icon='sync',
                        on_click=lambda: ctx.create_task(
                            service.engine.sync_core_repos(), name='iac:sync_core_repos'
                        ),
                    ).props('unelevated rounded size=sm color=blue-6') \
                     .bind_enabled_from(service.state, 'is_running', backward=lambda x: not x)

                ui.separator().classes('bg-zinc-800/40 my-1')

                ui.label('Clear all deployment statistics and job history. Running jobs are preserved.').classes(UIStyles.TEXT_MUTED)
                with ui.row().classes('w-full justify-end mt-2'):
                    ui.button('Clear All Stats', on_click=clear_stats_dialog.open, icon='delete_sweep', color='negative').props('unelevated rounded size=sm')
