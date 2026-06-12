#!/usr/bin/env python3
import sys
from datetime import date
from pathlib import Path

try:
    import tomllib
except ImportError:
    import tomli as tomllib

import click
import typer

from netaudio.cli import app


def get_version():
    for candidate in [Path(__file__).parent / "pyproject.toml", Path(__file__).resolve().parents[3] / "pyproject.toml"]:
        if candidate.exists():
            return tomllib.loads(candidate.read_text())["project"]["version"]
    from netaudio import __version__
    return __version__


def escape_troff(text):
    return text.replace("\\", "\\\\").replace("-", "\\-").replace(".", "\\.").replace("'", "\\(aq")


def format_option(param):
    parts = []
    decls = param.opts + param.secondary_opts
    is_flag = param.is_flag if hasattr(param, "is_flag") else False

    for decl in sorted(decls, key=lambda d: (not d.startswith("--"), d)):
        escaped = escape_troff(decl)
        if is_flag:
            parts.append(f"\\fB{escaped}\\fR")
        else:
            type_name = param.type.name.upper() if hasattr(param.type, "name") else "VALUE"
            if hasattr(param.type, "choices") and param.type.choices:
                type_name = "{" + "|".join(str(c) for c in param.type.choices) + "}"
            parts.append(f"\\fB{escaped}\\fR \\fI{escape_troff(type_name)}\\fR")

    line = ", ".join(parts)

    help_text = param.help or ""
    if param.default is not None and param.default != () and not is_flag:
        default_str = param.default.value if hasattr(param.default, "value") else str(param.default)
        if default_str and default_str != "None":
            help_text += f" [default: {default_str}]"

    if param.envvar:
        envs = param.envvar if isinstance(param.envvar, (list, tuple)) else [param.envvar]
        help_text += f" [env: {', '.join(envs)}]"

    return line, help_text


def format_argument(param):
    name = param.human_readable_name.upper()
    help_text = param.help if hasattr(param, "help") and param.help else ""
    required = param.required
    if not required:
        name = f"[{name}]"
    return name, help_text


def write_options(lines, params):
    options = [p for p in params if isinstance(p, click.Option) and p.name != "help"]
    if not options:
        return
    for param in options:
        decl, help_text = format_option(param)
        lines.append(f".TP")
        lines.append(decl)
        if help_text:
            lines.append(escape_troff(help_text))


def write_arguments(lines, params):
    args = [p for p in params if isinstance(p, click.Argument)]
    if not args:
        return
    for param in args:
        name, help_text = format_argument(param)
        lines.append(f".TP")
        lines.append(f"\\fI{escape_troff(name)}\\fR")
        if help_text:
            lines.append(escape_troff(help_text))


def synopsis_for(cmd, prefix):
    parts = [f"\\fB{escape_troff(prefix)}\\fR"]
    for param in cmd.params:
        if isinstance(param, click.Option) and param.name != "help":
            parts.append("[\\fIOPTIONS\\fR]")
            break
    for param in cmd.params:
        if isinstance(param, click.Argument):
            name = param.human_readable_name.upper()
            if param.required:
                parts.append(f"\\fI{escape_troff(name)}\\fR")
            else:
                parts.append(f"[\\fI{escape_troff(name)}\\fR]")
    return " ".join(parts)


def walk_commands(group, prefix="netaudio"):
    yield prefix, group
    if isinstance(group, click.Group):
        for name in group.list_commands(click.Context(group)):
            cmd = group.get_command(click.Context(group), name)
            if cmd is None:
                continue
            if getattr(cmd, "hidden", False):
                continue
            full = f"{prefix} {name}"
            yield from walk_commands(cmd, full)


def build_man_page():
    version = get_version()
    today = date.today().strftime("%Y\\-%m\\-%d")
    click_app = typer.main.get_command(app)

    lines = []

    lines.append(f'.TH NETAUDIO 1 "{today}" "{version}" "netaudio Manual"')
    lines.append(".SH NAME")
    lines.append("netaudio \\- manage and analyze network audio devices")

    lines.append(".SH SYNOPSIS")
    lines.append(".B netaudio")
    lines.append("[\\fIGLOBAL OPTIONS\\fR] \\fICOMMAND\\fR [\\fICOMMAND OPTIONS\\fR] [\\fIARGS\\fR]")

    lines.append(".SH DESCRIPTION")
    lines.append("netaudio discovers, configures, and monitors network audio devices. It supports Dante and Shure protocols, and can manage subscriptions, device settings, capture and replay traffic, analyze firmware, run a virtual Dante device, and maintain a protocol fact registry built from wire observations.")
    lines.append(".PP")
    lines.append("When invoked without a subcommand, netaudio lists all discovered devices.")

    lines.append(".SH GLOBAL OPTIONS")
    write_options(lines, click_app.params)

    top_groups = {}
    if isinstance(click_app, click.Group):
        for name in click_app.list_commands(click.Context(click_app)):
            cmd = click_app.get_command(click.Context(click_app), name)
            if cmd and not getattr(cmd, "hidden", False):
                top_groups[name] = cmd

    lines.append(".SH COMMANDS")

    for cmd_name, cmd in top_groups.items():
        lines.append(f".SS netaudio {escape_troff(cmd_name)}")
        help_text = cmd.get_short_help_str(limit=300) if hasattr(cmd, "get_short_help_str") else (cmd.help or "")
        if help_text:
            lines.append(escape_troff(help_text))

        if isinstance(cmd, click.Group):
            sub_names = cmd.list_commands(click.Context(cmd))
            for sub_name in sub_names:
                sub_cmd = cmd.get_command(click.Context(cmd), sub_name)
                if sub_cmd is None or getattr(sub_cmd, "hidden", False):
                    continue

                if isinstance(sub_cmd, click.Group):
                    subsub_names = sub_cmd.list_commands(click.Context(sub_cmd))
                    lines.append(f".PP")
                    lines.append(f".B {escape_troff(cmd_name)} {escape_troff(sub_name)}")
                    sub_help = sub_cmd.get_short_help_str(limit=300) if hasattr(sub_cmd, "get_short_help_str") else (sub_cmd.help or "")
                    if sub_help:
                        lines.append(f"\\- {escape_troff(sub_help)}")

                    for subsub_name in subsub_names:
                        subsub_cmd = sub_cmd.get_command(click.Context(sub_cmd), subsub_name)
                        if subsub_cmd is None or getattr(subsub_cmd, "hidden", False):
                            continue
                        full_prefix = f"{cmd_name} {sub_name} {subsub_name}"
                        lines.append(f".TP")
                        lines.append(synopsis_for(subsub_cmd, f"netaudio {full_prefix}"))
                        subsub_help = subsub_cmd.get_short_help_str(limit=300) if hasattr(subsub_cmd, "get_short_help_str") else (subsub_cmd.help or "")
                        if subsub_help:
                            lines.append(escape_troff(subsub_help))
                        write_arguments(lines, subsub_cmd.params)
                        write_options(lines, subsub_cmd.params)
                else:
                    full_prefix = f"{cmd_name} {sub_name}"
                    lines.append(f".TP")
                    lines.append(synopsis_for(sub_cmd, f"netaudio {full_prefix}"))
                    sub_help = sub_cmd.get_short_help_str(limit=300) if hasattr(sub_cmd, "get_short_help_str") else (sub_cmd.help or "")
                    if sub_help:
                        lines.append(escape_troff(sub_help))
                    write_arguments(lines, sub_cmd.params)
                    write_options(lines, sub_cmd.params)
        else:
            lines.append(f".TP")
            lines.append(synopsis_for(cmd, f"netaudio {cmd_name}"))
            write_arguments(lines, cmd.params)
            write_options(lines, cmd.params)

    lines.append(".SH ENVIRONMENT")
    lines.append("Global options can be set via environment variables:")
    env_vars = []
    for param in click_app.params:
        if isinstance(param, click.Option) and param.envvar:
            envs = param.envvar if isinstance(param.envvar, (list, tuple)) else [param.envvar]
            for env in envs:
                desc = param.help or param.name
                env_vars.append((env, desc))
    for env, desc in sorted(env_vars):
        lines.append(f".TP")
        lines.append(f"\\fB{escape_troff(env)}\\fR")
        lines.append(escape_troff(desc))

    lines.append(".SH FILES")
    lines.append(".TP")
    lines.append("\\fB~/.config/netaudio/config.toml\\fR")
    lines.append("User configuration file.")

    lines.append(".SH EXIT STATUS")
    lines.append(".TP")
    lines.append("\\fB0\\fR")
    lines.append("Success.")
    lines.append(".TP")
    lines.append("\\fB1\\fR")
    lines.append("General error.")
    lines.append(".TP")
    lines.append("\\fB2\\fR")
    lines.append("Usage error (bad arguments).")

    return "\n".join(lines) + "\n"


def main():
    target = Path(sys.argv[1]) if len(sys.argv) > 1 else Path("man")
    target.mkdir(parents=True, exist_ok=True)

    for old in target.glob("netaudio*.1"):
        old.unlink()

    man_content = build_man_page()
    (target / "netaudio.1").write_text(man_content)


if __name__ == "__main__":
    main()
