from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone


@dataclass(frozen=True)
class LockStatusObservation:
    lock_reset_status: dict
    observed_at: str

    @classmethod
    def from_lock_reset_status(cls, lock_reset_status: dict) -> LockStatusObservation:
        return cls(
            lock_reset_status=lock_reset_status,
            observed_at=datetime.now(timezone.utc).isoformat(timespec="microseconds"),
        )

    @property
    def lock_state_code(self) -> int:
        return self.lock_reset_status["lock_state_code"]

    @property
    def status_code(self) -> int:
        return self.lock_reset_status["status_code"]

    @property
    def is_locked(self) -> bool | None:
        return self.lock_reset_status["is_locked"]
