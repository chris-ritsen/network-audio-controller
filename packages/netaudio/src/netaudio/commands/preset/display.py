import typer

from netaudio.presets.loading import PresetLoadPlan


def show_preset_plan(plan: PresetLoadPlan) -> None:
    for entry in plan.device_actions:
        typer.echo(f"{entry.device_name} [{entry.server_name}]:")
        if not entry.actions:
            typer.echo("  no applicable settings")
            continue
        for action in entry.actions:
            label = action.kind.replace("_", " ")
            requested = action.payload
            if action.kind == "latency":
                requested = f"{action.payload:g} ms"
            elif action.kind == "sample_rate":
                requested = f"{action.payload} Hz"
            elif action.kind == "encoding":
                requested = f"{action.payload}-bit"
            current = "unavailable" if action.current is None else repr(action.current)
            detail = f" ({action.reason})" if action.reason else ""
            typer.echo(f"  {label}: {action.state.value}; {current} -> {requested}{detail}")
