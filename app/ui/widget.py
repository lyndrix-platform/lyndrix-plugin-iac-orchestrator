from nicegui import ui


def render_dashboard_widget(ctx, service):
    """Renders a status widget for the main dashboard."""
    state = service.state
    with ui.column().classes('gap-2 w-full'):
        ui.label("IaC Orchestrator").classes("text-base font-bold text-slate-200")
        ui.separator().classes('my-1 opacity-20')
        with ui.row().classes('w-full justify-between items-center'):
            ui.label("Last Deployment:").classes("text-xs text-slate-400")
            ui.label().classes("text-xs font-mono").bind_text_from(state, 'last_deployment')
        with ui.row().classes('w-full justify-between items-center'):
            ui.label("Pipeline Active:").classes("text-xs text-slate-400")
            with ui.row().classes('items-center gap-2'):
                ui.spinner('dots', color='indigo').bind_visibility_from(state, 'is_running')
                ui.label().classes("text-xs font-mono").bind_text_from(state, 'is_running', lambda v: "Yes" if v else "No")
