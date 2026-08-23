from __future__ import annotations

import os

DEFAULT_LOCALE = "en"

_catalogs: dict[str, dict[str, str]] = {}
_active_locale: str | None = None


def register_catalog(locale: str, mapping: dict[str, str]) -> None:
    normalized = _normalize_locale(locale)
    if not normalized:
        return
    catalog = _catalogs.setdefault(normalized, {})
    for message, translation in mapping.items():
        if isinstance(message, str) and isinstance(translation, str) and translation.strip():
            catalog[message] = translation


def _normalize_locale(locale: str | None) -> str | None:
    if not isinstance(locale, str) or not locale.strip():
        return None
    token = locale.strip().replace("-", "_")
    for separator in (".", "@"):
        if separator in token:
            token = token.split(separator, 1)[0]
    return token or None


def set_locale(locale: str | None) -> None:
    global _active_locale
    _active_locale = _normalize_locale(locale)


def get_locale() -> str:
    if _active_locale:
        return _active_locale
    for variable in ("NETAUDIO_LOCALE", "LC_ALL", "LC_MESSAGES", "LANG"):
        candidate = _normalize_locale(os.environ.get(variable))
        if candidate:
            return candidate
    return DEFAULT_LOCALE


def _lookup(locale: str, message: str) -> str | None:
    catalog = _catalogs.get(locale)
    if catalog is None and "_" in locale:
        catalog = _catalogs.get(locale.split("_", 1)[0])
    if catalog is not None:
        return catalog.get(message)
    return None


def translate(message: str | None, *, locale: str | None = None) -> str | None:
    if not message:
        return message

    resolved_locale = _normalize_locale(locale) or get_locale()
    if resolved_locale and resolved_locale != DEFAULT_LOCALE:
        translated = _lookup(resolved_locale, message)
        if translated:
            return translated

    return message


gettext = translate
_ = translate
