"""
Database models for Kriegspiel chess game using Peewee ORM.

This module defines the database schema for storing games and their history.
"""

import datetime
import json
import os
import sys
from typing import Optional

import bcrypt
import chess
from peewee import (
    SqliteDatabase,
    Model,
    AutoField,
    CharField,
    BooleanField,
    TextField,
    IntegerField,
    DateTimeField,
    ForeignKeyField,
)

# Add ks-game to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "ks-game"))

from kriegspiel.move import KriegspielMove, QuestionAnnouncement
from kriegspiel.serialization import serialize_berkeley_game, deserialize_berkeley_game

from kriegspiel_wrapper import ExtendedBerkeleyGame

# Database configuration - using SQLite for simplicity
DATABASE_PATH = "kriegspiel.db"
db = SqliteDatabase(DATABASE_PATH)


class BaseModel(Model):
    """Base model that all other models inherit from."""

    class Meta:
        database = db


class User(BaseModel):
    """
    User/Player model
    """

    id = AutoField(primary_key=True)
    name = CharField()
    email = CharField()
    password = CharField()

    def update_password(self, new_password: str) -> None:
        self.password = bcrypt.hashpw(new_password.encode("bytes"), bcrypt.gensalt(12))

    def check_password(self, attempt: str) -> bool:
        return bcrypt.checkpw(attempt.encode("bytes"), self.password)


class Game(BaseModel):
    """
    Represents a Kriegspiel chess game.

    Stores the core game information including current state,
    settings, and metadata.
    """

    id = AutoField(primary_key=True)
    game_id = CharField(unique=True, max_length=50, index=True)  # UUID from frontend
    white_player = ForeignKeyField(User, on_delete="CASCADE")
    black_player = ForeignKeyField(User, on_delete="CASCADE")
    any_rule = BooleanField(default=True)  # Whether ASK_ANY questions are allowed
    current_turn = CharField(max_length=5, choices=[("white", "White"), ("black", "Black")], default="white")
    is_game_over = BooleanField(default=False)
    board_fen = TextField()  # Current board state in FEN notation
    move_count = IntegerField(default=0)  # Total number of moves made
    game_state_json = TextField(null=True)  # Serialized game state from BerkeleyGame.save_game()
    created_at = DateTimeField(default=datetime.datetime.now)
    updated_at = DateTimeField(default=datetime.datetime.now)

    class Meta:
        table_name = "games"
        indexes = (
            # Index on game_id for fast lookups
            (("game_id",), True),  # Unique index
            # Index on created_at for chronological queries
            (("created_at",), False),
        )

    def save(self, *args, **kwargs):
        """Override save to update the updated_at timestamp."""
        self.updated_at = datetime.datetime.now()
        return super().save(*args, **kwargs)

    def __str__(self):
        return f"Game {self.game_id} ({self.current_turn} to move)"


class GameHistory(BaseModel):
    """
    Represents individual moves/questions in a Kriegspiel game.

    Stores the complete history of all questions asked and answers received
    during a game, enabling game replay and analysis.
    """

    id = AutoField(primary_key=True)
    game = ForeignKeyField(Game, backref="history", on_delete="CASCADE")
    move_number = IntegerField()  # Sequential move number within the game
    player = ForeignKeyField(User, on_delete="CASCADE")
    player_color = CharField(max_length=5, choices=[("white", "White"), ("black", "Black")])
    question_type = CharField(max_length=10, choices=[("COMMON", "Common Move"), ("ASK_ANY", "Ask Any")])
    move_uci = CharField(max_length=10, null=True)  # UCI notation (e.g., "e2e4"), null for ASK_ANY
    board_fen_before = TextField()  # Board state before the move
    board_fen_after = TextField(null=True)  # Board state after the move (null if illegal)

    # Answer from the game engine
    main_announcement = CharField(max_length=20)  # REGULAR_MOVE, CAPTURE_DONE, ILLEGAL_MOVE, etc.
    special_announcement = CharField(max_length=30, null=True)  # CHECK_RANK, CHECKMATE_WHITE_WINS, etc.
    capture_square = CharField(max_length=2, null=True)  # Square where capture occurred (e.g., "e4")
    is_legal = BooleanField()  # Whether the move was legal and executed
    has_any = BooleanField(null=True)  # For ASK_ANY questions: True/False/null

    timestamp = DateTimeField(default=datetime.datetime.now)

    class Meta:
        table_name = "game_history"
        indexes = (
            # Index on game for efficient history queries
            (("game", "move_number"), False),
            # Index on timestamp for chronological queries
            (("timestamp",), False),
        )

    def __str__(self):
        if self.question_type == "ASK_ANY":
            return f"Move {self.move_number}: {self.player} ASK_ANY -> {self.main_announcement}"
        else:
            return f"Move {self.move_number}: {self.player} {self.move_uci} -> {self.main_announcement}"


def create_tables():
    """Create all database tables if they don't exist."""
    with db:
        db.create_tables([Game, GameHistory])


def migrate_database():
    """Run database migrations to add new fields."""
    # Check if game_state_json column exists
    try:
        # Try to access the column - if it doesn't exist, this will fail
        db.execute_sql("SELECT game_state_json FROM games LIMIT 1")
    except Exception:
        # Column doesn't exist, add it
        print("Adding game_state_json column to games table...")
        db.execute_sql("ALTER TABLE games ADD COLUMN game_state_json TEXT")
        print("Migration completed successfully")


def initialize_database():
    """Initialize the database connection and create tables."""
    db.connect()
    create_tables()
    migrate_database()
    return db


def close_database():
    """Close the database connection."""
    if not db.is_closed():
        db.close()


# Optional: Add some helper functions for common operations
def get_game_by_id(game_id: str) -> Optional[Game]:
    """Get a game by its UUID."""
    try:
        return Game.get(Game.game_id == game_id)
    except Game.DoesNotExist:
        return None


def get_game_history(game_id: str) -> list:
    """Get the complete history of moves for a game."""
    game = get_game_by_id(game_id)
    if not game:
        return []

    return GameHistory.select().where(GameHistory.game == game).order_by(GameHistory.move_number)


def save_game_state(
    game_id: str,
    board_fen: str,
    current_turn: str,
    is_game_over: bool = False,
    move_count: int = 0,
    game_state_json: Optional[str] = None,
) -> Game:
    """Save or update the current game state."""
    game, created = Game.get_or_create(
        game_id=game_id,
        defaults={
            "board_fen": board_fen,
            "current_turn": current_turn,
            "is_game_over": is_game_over,
            "move_count": move_count,
            "game_state_json": game_state_json,
        },
    )

    if not created:
        # Update existing game
        game.board_fen = board_fen
        game.current_turn = current_turn
        game.is_game_over = is_game_over
        game.move_count = move_count
        game.game_state_json = game_state_json
        game.save()

    return game


def save_move_history(
    game_id: str,
    move_number: int,
    player: str,
    question_type: str,
    move_uci: Optional[str],
    board_fen_before: str,
    board_fen_after: Optional[str],
    main_announcement: str,
    special_announcement: Optional[str] = None,
    capture_square: Optional[str] = None,
    is_legal: bool = True,
    has_any: Optional[bool] = None,
) -> GameHistory:
    """Save a move to the game history."""
    game = get_game_by_id(game_id)
    if not game:
        raise ValueError(f"Game {game_id} not found")

    return GameHistory.create(
        game=game,
        move_number=move_number,
        player=player,
        question_type=question_type,
        move_uci=move_uci,
        board_fen_before=board_fen_before,
        board_fen_after=board_fen_after,
        main_announcement=main_announcement,
        special_announcement=special_announcement,
        capture_square=capture_square,
        is_legal=is_legal,
        has_any=has_any,
    )


def reconstruct_game_from_history(game_id: str) -> ExtendedBerkeleyGame:
    """
    Reconstruct a BerkeleyGame from database history using JSON serialization.

    This function replays all moves from the database to recreate the exact
    game state, including scoresheets and internal state.

    Args:
        game_id: The UUID of the game to reconstruct

    Returns:
        ExtendedBerkeleyGame: Reconstructed game instance

    Raises:
        ValueError: If game not found or reconstruction fails
    """
    # Get game from database
    db_game = get_game_by_id(game_id)
    if not db_game:
        raise ValueError(f"Game {game_id} not found")

    # Get game history ordered by move number
    history = GameHistory.select().where(GameHistory.game == db_game).order_by(GameHistory.move_number, GameHistory.timestamp)

    if not history:
        # No moves yet, just create a fresh game
        return ExtendedBerkeleyGame(any_rule=db_game.any_rule)

    # Start with a fresh game
    game = ExtendedBerkeleyGame(any_rule=db_game.any_rule)

    # Replay all moves to reconstruct the exact state
    for move_entry in history:
        try:
            # Create the KriegspielMove
            if move_entry.question_type == "ASK_ANY":
                ks_move = KriegspielMove(QuestionAnnouncement.ASK_ANY, None)
            else:
                if move_entry.move_uci:
                    chess_move = chess.Move.from_uci(move_entry.move_uci)
                    ks_move = KriegspielMove(QuestionAnnouncement.COMMON, chess_move)
                else:
                    continue  # Skip invalid moves

            # Apply the move to reconstruct state
            answer = game.ask_for(ks_move)

            # Verify the answer matches what was recorded (optional validation)
            if answer.main_announcement.name != move_entry.main_announcement:
                print(
                    f"Warning: Answer mismatch for move {move_entry.move_number}: "
                    f"expected {move_entry.main_announcement}, got {answer.main_announcement.name}"
                )

        except Exception as e:
            print(f"Error reconstructing move {move_entry.move_number}: {e}")
            # Continue with partial reconstruction
            continue

    return game


def serialize_game_to_json(game: ExtendedBerkeleyGame) -> str:
    """
    Serialize a BerkeleyGame instance to JSON string.

    Args:
        game: ExtendedBerkeleyGame instance to serialize

    Returns:
        str: JSON string containing the serialized game state
    """
    game_data = serialize_berkeley_game(game._game)
    return json.dumps(game_data)


def deserialize_game_from_json(json_str: str) -> ExtendedBerkeleyGame:
    """
    Deserialize a BerkeleyGame instance from JSON string.

    Args:
        json_str: JSON string containing the serialized game state

    Returns:
        ExtendedBerkeleyGame: Restored game instance

    Raises:
        ValueError: If the JSON is invalid or game reconstruction fails
    """
    try:
        game_data = json.loads(json_str)
        berkeley_game = deserialize_berkeley_game(game_data)

        # Wrap in ExtendedBerkeleyGame
        extended_game = ExtendedBerkeleyGame.__new__(ExtendedBerkeleyGame)
        extended_game._game = berkeley_game
        return extended_game
    except (json.JSONDecodeError, Exception) as e:
        raise ValueError(f"Failed to deserialize game from JSON: {e}")


def save_game_with_serialization(game_id: str, game: ExtendedBerkeleyGame) -> Game:
    """
    Save game state using JSON serialization.

    Args:
        game_id: Game UUID
        game: ExtendedBerkeleyGame instance to save

    Returns:
        Game: Database game record
    """
    game_state_json = serialize_game_to_json(game)
    board_fen = game._game._board.fen()
    current_turn = "white" if game.turn == chess.WHITE else "black"
    is_game_over = game.game_over
    move_count = game._game._board.fullmove_number

    return save_game_state(
        game_id=game_id,
        board_fen=board_fen,
        current_turn=current_turn,
        is_game_over=is_game_over,
        move_count=move_count,
        game_state_json=game_state_json,
    )


def load_game_from_serialization(game_id: str) -> Optional[ExtendedBerkeleyGame]:
    """
    Load game from JSON serialization if available.

    Args:
        game_id: Game UUID

    Returns:
        ExtendedBerkeleyGame or None: Restored game instance or None if not available
    """
    db_game = get_game_by_id(game_id)
    if not db_game or not db_game.game_state_json:
        return None

    try:
        return deserialize_game_from_json(db_game.game_state_json)
    except ValueError:
        # If JSON deserialization fails, return None to fall back to history reconstruction
        return None
