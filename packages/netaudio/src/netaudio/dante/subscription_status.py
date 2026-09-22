from __future__ import annotations

from netaudio.core import subscription_classification_for_identifier


_MANAGED_MESSAGE_PREFIXES = ("error:", "warning:", "info:")


def humanize_managed_status(status) -> str:
    if not isinstance(status, str) or not status.strip():
        return "Status unavailable"
    return status.strip().replace("_", " ").capitalize()


def clean_managed_message(status_message) -> str | None:
    if not isinstance(status_message, str):
        return None
    message = status_message.strip()
    lowered = message.casefold()
    for prefix in _MANAGED_MESSAGE_PREFIXES:
        if lowered.startswith(prefix):
            message = message[len(prefix) :].strip()
            break
    return message or None


def managed_status_presentation(status, status_message, summary) -> tuple[str, str | None]:
    message = clean_managed_message(status_message)
    identifier = status.strip().upper() if isinstance(status, str) and status.strip() else None
    presentation = subscription_classification_for_identifier(identifier)

    if presentation["label"] is not None:
        return presentation["label"], presentation["detail"] or message

    normalized_summary = summary.strip().casefold() if isinstance(summary, str) and summary.strip() else None
    if identifier:
        return humanize_managed_status(identifier), message
    if normalized_summary == "connected":
        return "Connected", message
    if normalized_summary:
        return humanize_managed_status(summary), message
    return "Status unavailable", message
