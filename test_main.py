import pytest
import os
import sys
import tempfile
from fastapi.testclient import TestClient
from main import app
from models import db, initialize_database, close_database

# Add ks-game to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "ks-game"))

import chess
from kriegspiel_wrapper import ExtendedBerkeleyGame
from kriegspiel.move import KriegspielMove, QuestionAnnouncement


@pytest.fixture(scope="function")
def test_client():
    """Create a test client with a temporary database for each test."""
    # Create a temporary file for the test database
    temp_file = tempfile.NamedTemporaryFile(delete=False, suffix=".db")
    temp_db_path = temp_file.name
    temp_file.close()

    # Close existing database connection
    if not db.is_closed():
        db.close()

    # Reinitialize database with temporary path
    db.init(temp_db_path)
    initialize_database()

    # Create test client
    client = TestClient(app)

    yield client

    # Cleanup
    close_database()
    os.unlink(temp_db_path)


def test_root(test_client):
    response = test_client.get("/")
    assert response.status_code == 200
    assert response.json() == {"message": "Kriegspiel Chess API"}


def test_create_game(test_client):
    response = test_client.post("/games")
    assert response.status_code == 200
    data = response.json()
    assert "game_id" in data
    assert data["status"] == "created"
    assert data["any_rule"] is True
    assert len(data["game_id"]) > 0


def test_create_game_with_any_rule_false(test_client):
    response = test_client.post("/games?any_rule=false")
    assert response.status_code == 200
    data = response.json()
    assert data["any_rule"] is False


def test_get_game_state_nonexistent(test_client):
    response = test_client.get("/games/nonexistent?player=white")
    assert response.status_code == 404
    assert response.json()["detail"] == "Game not found"


def test_get_game_state_invalid_player(test_client):
    create_response = test_client.post("/games")
    game_id = create_response.json()["game_id"]

    response = test_client.get(f"/games/{game_id}?player=invalid")
    assert response.status_code == 400
    assert response.json()["detail"] == "Player must be 'white' or 'black'"


def test_get_game_state_valid(test_client):
    create_response = test_client.post("/games")
    game_id = create_response.json()["game_id"]

    response = test_client.get(f"/games/{game_id}?player=white")
    assert response.status_code == 200
    data = response.json()
    assert data["game_id"] == game_id
    assert data["player"] == "white"
    assert "visible_board" in data
    assert "board_fen" in data
    assert data["turn"] == "white"
    assert data["is_game_over"] is False


def test_initial_board_position(test_client):
    # Test that the game starts with standard chess position
    create_response = test_client.post("/games")
    game_id = create_response.json()["game_id"]

    # Test white's view
    white_response = test_client.get(f"/games/{game_id}?player=white")
    white_data = white_response.json()
    white_fen = white_data["board_fen"]

    # White should see only white pieces in starting position
    # Check the board part of the FEN (before the space)
    white_board_part = white_fen.split()[0]
    expected_white_board = "8/8/8/8/8/8/PPPPPPPP/RNBQKBNR"
    assert white_board_part == expected_white_board

    # Verify it's white's turn
    assert white_fen.split()[1] == "w"

    # Test black's view
    black_response = test_client.get(f"/games/{game_id}?player=black")
    black_data = black_response.json()
    black_fen = black_data["board_fen"]

    # Black should see only black pieces in starting position
    black_board_part = black_fen.split()[0]
    expected_black_board = "rnbqkbnr/pppppppp/8/8/8/8/8/8"
    assert black_board_part == expected_black_board

    # Should still be white's turn from black's perspective too
    assert black_fen.split()[1] == "w"


def test_board_state_after_move(test_client):
    # Test that board state updates correctly after a move
    create_response = test_client.post("/games")
    game_id = create_response.json()["game_id"]

    # Make a move as white (e2e4)
    move_response = test_client.post(f"/games/{game_id}/move?player=white&move_uci=e2e4")
    assert move_response.status_code == 200
    move_data = move_response.json()
    assert move_data["legal"] is True

    # Check that the move is reflected in the board FEN
    assert "board_fen" in move_data
    # After e2e4, white should see their pawn on e4
    fen_after_move = move_data["board_fen"]
    # The FEN should show the pawn moved from e2 to e4
    assert "PPPPPPPP" not in fen_after_move  # e2 pawn moved
    assert "4P3" in fen_after_move  # pawn on e4


def test_pieces_visibility_kriegspiel(test_client):
    # Test that players only see their own pieces
    create_response = test_client.post("/games")
    game_id = create_response.json()["game_id"]

    # Get white's initial view
    white_response = test_client.get(f"/games/{game_id}?player=white")
    white_fen = white_response.json()["board_fen"]

    # Get black's initial view
    black_response = test_client.get(f"/games/{game_id}?player=black")
    black_fen = black_response.json()["board_fen"]

    # Extract only the board position part (before first space)
    white_board_part = white_fen.split()[0]
    black_board_part = black_fen.split()[0]

    # White board should contain only uppercase letters (white pieces)
    white_pieces = [c for c in white_board_part if c.isalpha()]
    assert all(c.isupper() for c in white_pieces), f"White should only see white pieces (uppercase), got: {white_pieces}"

    # Black board should contain only lowercase letters (black pieces)
    black_pieces = [c for c in black_board_part if c.isalpha()]
    assert all(c.islower() for c in black_pieces), f"Black should only see black pieces (lowercase), got: {black_pieces}"


def test_make_move_nonexistent_game(test_client):
    response = test_client.post("/games/nonexistent/move?player=white&move_uci=e2e4")
    assert response.status_code == 404


def test_make_move_invalid_player(test_client):
    create_response = test_client.post("/games")
    game_id = create_response.json()["game_id"]

    response = test_client.post(f"/games/{game_id}/move?player=invalid&move_uci=e2e4")
    assert response.status_code == 400


def test_make_move_wrong_turn(test_client):
    create_response = test_client.post("/games")
    game_id = create_response.json()["game_id"]

    response = test_client.post(f"/games/{game_id}/move?player=black&move_uci=e7e5")
    assert response.status_code == 400
    assert response.json()["detail"] == "Not your turn"


def test_make_valid_move(test_client):
    create_response = test_client.post("/games")
    game_id = create_response.json()["game_id"]

    response = test_client.post(f"/games/{game_id}/move?player=white&move_uci=e2e4")
    assert response.status_code == 200
    data = response.json()
    assert data["game_id"] == game_id
    assert data["player"] == "white"
    assert data["move"] == "e2e4"
    assert "legal" in data
    assert "visible_board" in data


def test_make_invalid_move(test_client):
    create_response = test_client.post("/games")
    game_id = create_response.json()["game_id"]

    # Test with an illegal move that should return legal=false
    response = test_client.post(f"/games/{game_id}/move?player=white&move_uci=a1a2")
    assert response.status_code == 200
    data = response.json()
    assert data["legal"] is False  # Kriegspiel returns illegal moves as legal=false


def test_delete_game(test_client):
    create_response = test_client.post("/games")
    game_id = create_response.json()["game_id"]

    response = test_client.delete(f"/games/{game_id}")
    assert response.status_code == 200
    assert response.json()["message"] == "Game deleted"

    # Verify game is actually deleted
    get_response = test_client.get(f"/games/{game_id}?player=white")
    assert get_response.status_code == 404


def test_delete_nonexistent_game(test_client):
    response = test_client.delete("/games/nonexistent")
    assert response.status_code == 404


def test_game_flow(test_client):
    # Create game
    create_response = test_client.post("/games")
    game_id = create_response.json()["game_id"]

    # Make a move as white
    white_move = test_client.post(f"/games/{game_id}/move?player=white&move_uci=e2e4")
    assert white_move.status_code == 200

    # Check game state shows black to move
    state_response = test_client.get(f"/games/{game_id}?player=black")
    assert state_response.status_code == 200
    data = state_response.json()
    assert data["turn"] == "black"

    # Make a move as black
    black_move = test_client.post(f"/games/{game_id}/move?player=black&move_uci=e7e5")
    assert black_move.status_code == 200

    # Verify turn is back to white
    final_state = test_client.get(f"/games/{game_id}?player=white")
    assert final_state.status_code == 200
    assert final_state.json()["turn"] == "white"


def test_ask_any_pawn_capture_scenario(test_client):
    """Test the specific scenario: b2b4, c7c5, then ASK_ANY should find captures"""
    # Create game
    create_response = test_client.post("/games")
    game_id = create_response.json()["game_id"]

    # White plays b2b4
    white_move1 = test_client.post(f"/games/{game_id}/move?player=white&move_uci=b2b4")
    assert white_move1.status_code == 200
    white_data1 = white_move1.json()
    assert white_data1["legal"] is True
    print(f"After b2b4 - White's response: {white_data1}")

    # Black plays c7c5
    black_move1 = test_client.post(f"/games/{game_id}/move?player=black&move_uci=c7c5")
    assert black_move1.status_code == 200
    black_data1 = black_move1.json()
    assert black_data1["legal"] is True
    print(f"After c7c5 - Black's response: {black_data1}")

    # Now white should be able to ask ASK_ANY and get HAS_ANY (bxc5 should be possible)
    # Use the move endpoint with question_type=ASK_ANY and no move_uci needed
    ask_any_response = test_client.post(f"/games/{game_id}/move?player=white&question_type=ASK_ANY")
    assert ask_any_response.status_code == 200
    ask_any_data = ask_any_response.json()
    print(f"ASK_ANY response: {ask_any_data}")

    # This should return has_any=True because white pawn on b4 can capture black pawn on c5
    assert ask_any_data["has_any"] is True, f"Expected has_any=True but got {ask_any_data}"


def test_berkeley_game_direct_pawn_capture_scenario():
    """Test the BerkeleyGame directly to ensure the logic works"""
    # Create a game directly
    game = ExtendedBerkeleyGame(any_rule=True)

    # White plays b2b4
    white_move1 = KriegspielMove(QuestionAnnouncement.COMMON, chess.Move.from_uci("b2b4"))
    answer1 = game.ask_for(white_move1)
    print(f"After b2b4 - Answer: {answer1}")
    assert answer1.main_announcement.name == "REGULAR_MOVE"

    # Black plays c7c5
    black_move1 = KriegspielMove(QuestionAnnouncement.COMMON, chess.Move.from_uci("c7c5"))
    answer2 = game.ask_for(black_move1)
    print(f"After c7c5 - Answer: {answer2}")
    assert answer2.main_announcement.name == "REGULAR_MOVE"

    # Now it should be white's turn, test ASK_ANY
    assert game.turn == chess.WHITE

    # White asks ASK_ANY
    ask_any_move = KriegspielMove(QuestionAnnouncement.ASK_ANY, None)
    answer3 = game.ask_for(ask_any_move)
    print(f"ASK_ANY response: {answer3}")
    print(f"Answer announcement: {answer3.main_announcement.name}")

    # Should return HAS_ANY because b4 pawn can capture c5 pawn
    assert (
        answer3.main_announcement.name == "HAS_ANY"
    ), f"Expected HAS_ANY but got {answer3.main_announcement.name}. Answer: {answer3}"


def test_game_history_endpoint(test_client):
    """Test the game history endpoint returns move history"""
    # Create game
    create_response = test_client.post("/games")
    game_id = create_response.json()["game_id"]

    # Make a few moves
    white_move = test_client.post(f"/games/{game_id}/move?player=white&move_uci=e2e4")
    assert white_move.status_code == 200

    black_move = test_client.post(f"/games/{game_id}/move?player=black&move_uci=e7e5")
    assert black_move.status_code == 200

    # Ask ASK_ANY
    ask_any = test_client.post(f"/games/{game_id}/move?player=white&question_type=ASK_ANY")
    assert ask_any.status_code == 200

    # Get history
    history_response = test_client.get(f"/games/{game_id}/history")
    assert history_response.status_code == 200

    history_data = history_response.json()
    assert history_data["game_id"] == game_id
    assert "history" in history_data

    history = history_data["history"]
    assert len(history) >= 3  # At least the 3 moves we made

    # Check first move
    assert history[0]["player"] == "white"
    assert history[0]["question_type"] == "COMMON"
    assert history[0]["move_uci"] == "e2e4"
    assert history[0]["main_announcement"] == "REGULAR_MOVE"
    assert history[0]["is_legal"] is True

    # Check ASK_ANY move
    ask_any_move = next((h for h in history if h["question_type"] == "ASK_ANY"), None)
    assert ask_any_move is not None
    assert ask_any_move["move_uci"] is None
    assert ask_any_move["has_any"] is not None  # Should be True or False


def test_game_history_nonexistent_game(test_client):
    """Test game history for nonexistent game returns 404"""
    response = test_client.get("/games/nonexistent/history")
    assert response.status_code == 404


def test_game_reconstruction_from_database(test_client):
    """Test that games can be reconstructed from database after server restart simulation"""
    # Create game and make some moves
    create_response = test_client.post("/games")
    game_id = create_response.json()["game_id"]

    # Make a few moves to create history
    white_move1 = test_client.post(f"/games/{game_id}/move?player=white&move_uci=e2e4")
    assert white_move1.status_code == 200
    assert white_move1.json()["legal"] is True

    black_move1 = test_client.post(f"/games/{game_id}/move?player=black&move_uci=e7e5")
    assert black_move1.status_code == 200
    assert black_move1.json()["legal"] is True

    white_move2 = test_client.post(f"/games/{game_id}/move?player=white&move_uci=g1f3")
    assert white_move2.status_code == 200
    assert white_move2.json()["legal"] is True

    # Get game state to verify it works
    state_before = test_client.get(f"/games/{game_id}?player=white")
    assert state_before.status_code == 200
    white_fen_before = state_before.json()["board_fen"]

    # Simulate server restart by clearing the games dictionary
    from main import games

    games.clear()

    # Verify game is no longer in memory
    assert game_id not in games

    # Try to get game state - should trigger reconstruction
    state_after = test_client.get(f"/games/{game_id}?player=white")
    assert state_after.status_code == 200

    # Verify game was reconstructed correctly
    assert game_id in games
    white_fen_after = state_after.json()["board_fen"]

    # The reconstructed game should have the same state
    assert white_fen_after == white_fen_before
    assert state_after.json()["turn"] == "black"  # Should be black's turn after 3 moves

    # Test making a move on the reconstructed game
    black_move2 = test_client.post(f"/games/{game_id}/move?player=black&move_uci=b8c6")
    assert black_move2.status_code == 200
    assert black_move2.json()["legal"] is True


def test_game_reconstruction_move_endpoint(test_client):
    """Test that move endpoint also triggers reconstruction"""
    # Create game and make a move
    create_response = test_client.post("/games")
    game_id = create_response.json()["game_id"]

    white_move1 = test_client.post(f"/games/{game_id}/move?player=white&move_uci=d2d4")
    assert white_move1.status_code == 200

    # Clear games from memory
    from main import games

    games.clear()
    assert game_id not in games

    # Try to make a move - should trigger reconstruction
    black_move1 = test_client.post(f"/games/{game_id}/move?player=black&move_uci=d7d5")
    assert black_move1.status_code == 200
    assert black_move1.json()["legal"] is True

    # Verify game is back in memory
    assert game_id in games
