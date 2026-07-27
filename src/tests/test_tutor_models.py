from __future__ import annotations

from app.models.tutor import TutorFeedbackRequest


def test_tutor_feedback_comment_is_trimmed_and_empty_comments_become_none() -> None:
    assert TutorFeedbackRequest(rating="helpful", comment="  useful  ").comment == "useful"
    assert TutorFeedbackRequest(rating="not_helpful", comment="   ").comment is None
    assert TutorFeedbackRequest(rating="incorrect").comment is None
