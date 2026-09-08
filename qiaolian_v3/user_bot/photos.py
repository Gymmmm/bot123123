from __future__ import annotations

from .repository import UserListingRepository


class PhotoService:
    def __init__(self, repository: UserListingRepository) -> None:
        self.repository = repository

    def more_photos(self, public_listing_id: str) -> tuple[str, ...]:
        return self.repository.approved_gallery(public_listing_id)


__all__ = ['PhotoService']
