from __future__ import annotations

from .storage import ReviewStorage


class ProfileService:
    def __init__(self, storage: ReviewStorage | None = None):
        self.storage = storage or ReviewStorage()

    def list_profiles(self):
        return self.storage.list_profiles()

    def get_active_profile_id(self) -> str:
        return self.storage.get_active_profile_id()

    def create_profile(self, display_name: str):
        return self.storage.create_profile(display_name)

    def switch_profile(self, profile_id: str) -> None:
        self.storage.set_active_profile(profile_id)

    def reset_profile(self, profile_id: str) -> None:
        self.storage.reset_profile(profile_id)

    def delete_profile(self, profile_id: str) -> None:
        self.storage.delete_profile(profile_id)
