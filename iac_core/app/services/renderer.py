from typing import Any, Dict
from jinja2 import Environment, Undefined
from core.logger import get_logger

log = get_logger("IaC:Generator:Renderer")

# Variables that are only known at DEPLOY time (per-host Ansible run), not during this
# global, host-agnostic inventory-generation render. They MUST survive this stage as
# literal Jinja so the downstream template engine — which has host scope and injects
# these — can resolve them per host. Without this, PermissiveUndefined collapses them to
# '' and bakes an empty value into the generated inventory
# (e.g. PDNS_HOST_IP: '{{ ansible_host_ip }}' -> '').
DEPLOY_TIME_PASSTHROUGH = ("ansible_host_ip", "inventory_hostname")

class PermissiveUndefined(Undefined):
    """Returns an empty string for missing variables to prevent hard crashes during evaluation."""
    def _fail_with_undefined_error(self, *args, **kwargs):
        log.debug(f"Undefined variable encountered: {self._undefined_name}")
        return ""

def _render_pass(data: Any, context: Dict[str, Any], env: Environment) -> Any:
    """Recursively processes data and renders Jinja2 strings."""
    if isinstance(data, dict):
        return {k: _render_pass(v, context, env) for k, v in data.items()}
    elif isinstance(data, list):
        return [_render_pass(i, context, env) for i in data]
    elif isinstance(data, str) and '{{' in data and '}}' in data:
        try:
            template = env.from_string(data)
            return template.render(context)
        except Exception as e:
            log.warning(f"Could not render template '{data}': {e}")
            return data
    return data

def _build_flat_context(config: Dict[str, Any]) -> Dict[str, Any]:
    """
    Baut einen flachen Kontext, um Ansible's Variablen-Verhalten zu simulieren.
    Holt alle Keys aus global_vars, site_vars und stage_vars auf die Root-Ebene.
    """
    context = {}
    for key in ["global_vars", "site_vars", "stage_vars"]:
        if key in config and isinstance(config[key], dict):
            context.update(config[key])
            
    context.update(config)
    # Hilfsvariable, da in deiner 01_global_vars.yml "toplevel_vars.checkmk_commons" genutzt wird
    context['toplevel_vars'] = config
    # Preserve deploy-time variables as literal Jinja so they pass through untouched
    # instead of being blanked by PermissiveUndefined (see DEPLOY_TIME_PASSTHROUGH).
    for var in DEPLOY_TIME_PASSTHROUGH:
        context[var] = "{{ " + var + " }}"
    return context

def render_templates(config: Dict[str, Any]) -> Dict[str, Any]:
    """
    Renders Jinja2 templates embedded in the configuration dictionary.
    Executes exactly two passes to resolve nested references without infinite loops.
    """
    env = Environment(undefined=PermissiveUndefined)
    
    # Pass 1: Resolve primary templates
    context1 = _build_flat_context(config)
    first_pass = _render_pass(config, context1, env)
    
    # Pass 2: Resolve templates that depended on Pass 1 outcomes
    context2 = _build_flat_context(first_pass)
    final_pass = _render_pass(first_pass, context2, env)
    
    return final_pass