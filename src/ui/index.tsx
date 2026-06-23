import PluginApp from './PluginApp'

// Explicit global assignment — more reliable than relying on Rollup's IIFE
// named-export → window property mechanism in strict-mode bundles. The global
// name must match usePluginModules.ts: __lyndrix_plugin_<id with . and - → _>.
;(window as Record<string, unknown>)['__lyndrix_plugin_lyndrix_plugin_iac_orchestrator'] = {
  PluginApp,
  pluginRoutes: [
    { path: '/iac', label: 'IaC Orchestrator', icon: 'rocket_launch', sidebar_visible: true },
    { path: '/iac/settings', label: 'IaC Orchestrator Settings', icon: 'settings', sidebar_visible: false },
  ],
}
