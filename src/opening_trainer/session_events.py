from __future__ import annotations

from dataclasses import asdict

from .session_contracts import SessionEvent


def build_event(event_type: str, **payload) -> SessionEvent:
    return SessionEvent(event_type=event_type, payload=payload)


def event_to_dict(event: SessionEvent) -> dict:
    return asdict(event)
