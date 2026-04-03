from datetime import UTC, datetime

from bson import ObjectId

from app.models.user import UserModel


def test_user_model_accepts_bot_join_cooldown_timestamp() -> None:
    now = datetime.now(UTC)

    user = UserModel.from_mongo(
        {
            "_id": ObjectId(),
            "username": "randobot",
            "username_display": "randobot",
            "email": "bot@example.com",
            "password_hash": "hash",
            "role": "bot",
            "status": "active",
            "last_active_at": now,
            "created_at": now,
            "updated_at": now,
            "bot_profile": {
                "display_name": "Random Bot",
                "description": "Bot",
                "listed": True,
                "last_bot_game_joined_at": now,
            },
        }
    )

    assert user.bot_profile is not None
    assert user.bot_profile.last_bot_game_joined_at == now
