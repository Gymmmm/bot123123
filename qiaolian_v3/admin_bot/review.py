from __future__ import annotations

from qiaolian_v3.db.repositories.reviews import ReviewRepository


class AdminReviewService:
    def __init__(self, repository: ReviewRepository) -> None:
        self.repository = repository

    def open_items(self):
        return self.repository.list_open()

    def resolve(self, review_id: int, *, action: str, operator_id: str, note: str = ''):
        status = {'approve': 'approved', 'reject': 'rejected', 'resolve': 'resolved'}.get(str(action))
        if status is None:
            raise ValueError('invalid_review_action')
        return self.repository.resolve(
            review_id, status=status, operator_id=operator_id,
            resolution={'action': action, 'note': note},
        )


__all__ = ['AdminReviewService']
