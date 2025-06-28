from fastapi.testclient import TestClient
from main import app

client = TestClient(app)


def test_root():
    response = client.get("/")
    assert response.status_code == 200
    assert response.json() == {"message": "Kriegspiel Chess API"}


def test_create_game():
    response = client.post("/games")
    assert response.status_code == 200
    data = response.json()
    assert "game_id" in data
    assert data["status"] == "created"
    assert data["any_rule"] is True
    assert len(data["game_id"]) > 0


def test_create_game_with_any_rule_false():
    response = client.post("/games?any_rule=false")
    assert response.status_code == 200
    data = response.json()
    assert data["any_rule"] is False


def test_get_game_state_nonexistent():
    response = client.get("/games/nonexistent?player=white")
    assert response.status_code == 404
    assert response.json()["detail"] == "Game not found"


def test_get_game_state_invalid_player():
    create_response = client.post("/games")
    game_id = create_response.json()["game_id"]

    response = client.get(f"/games/{game_id}?player=invalid")
    assert response.status_code == 400
    assert response.json()["detail"] == "Player must be 'white' or 'black'"


def test_get_game_state_valid():
    create_response = client.post("/games")
    game_id = create_response.json()["game_id"]

    response = client.get(f"/games/{game_id}?player=white")
    assert response.status_code == 200
    data = response.json()
    assert data["game_id"] == game_id
    assert data["player"] == "white"
    assert "visible_board" in data
    assert "board_fen" in data
    assert data["turn"] == "white"
    assert data["is_game_over"] is False


def test_initial_board_position():
    # Test that the game starts with standard chess position
    create_response = client.post("/games")
    game_id = create_response.json()["game_id"]

    # Test white's view
    white_response = client.get(f"/games/{game_id}?player=white")
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
    black_response = client.get(f"/games/{game_id}?player=black")
    black_data = black_response.json()
    black_fen = black_data["board_fen"]

    # Black should see only black pieces in starting position
    black_board_part = black_fen.split()[0]
    expected_black_board = "rnbqkbnr/pppppppp/8/8/8/8/8/8"
    assert black_board_part == expected_black_board

    # Should still be white's turn from black's perspective too
    assert black_fen.split()[1] == "w"


def test_board_state_after_move():
    # Test that board state updates correctly after a move
    create_response = client.post("/games")
    game_id = create_response.json()["game_id"]

    # Make a move as white (e2e4)
    move_response = client.post(f"/games/{game_id}/move?player=white&move_uci=e2e4")
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


def test_pieces_visibility_kriegspiel():
    # Test that players only see their own pieces
    create_response = client.post("/games")
    game_id = create_response.json()["game_id"]

    # Get white's initial view
    white_response = client.get(f"/games/{game_id}?player=white")
    white_fen = white_response.json()["board_fen"]

    # Get black's initial view
    black_response = client.get(f"/games/{game_id}?player=black")
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


def test_make_move_nonexistent_game():
    response = client.post("/games/nonexistent/move?player=white&move_uci=e2e4")
    assert response.status_code == 404


def test_make_move_invalid_player():
    create_response = client.post("/games")
    game_id = create_response.json()["game_id"]

    response = client.post(f"/games/{game_id}/move?player=invalid&move_uci=e2e4")
    assert response.status_code == 400


def test_make_move_wrong_turn():
    create_response = client.post("/games")
    game_id = create_response.json()["game_id"]

    response = client.post(f"/games/{game_id}/move?player=black&move_uci=e7e5")
    assert response.status_code == 400
    assert response.json()["detail"] == "Not your turn"


def test_make_valid_move():
    create_response = client.post("/games")
    game_id = create_response.json()["game_id"]

    response = client.post(f"/games/{game_id}/move?player=white&move_uci=e2e4")
    assert response.status_code == 200
    data = response.json()
    assert data["game_id"] == game_id
    assert data["player"] == "white"
    assert data["move"] == "e2e4"
    assert "legal" in data
    assert "visible_board" in data


def test_make_invalid_move():
    create_response = client.post("/games")
    game_id = create_response.json()["game_id"]

    # Test with an illegal move that should return legal=false
    response = client.post(f"/games/{game_id}/move?player=white&move_uci=a1a2")
    assert response.status_code == 200
    data = response.json()
    assert data["legal"] is False  # Kriegspiel returns illegal moves as legal=false


def test_delete_game():
    create_response = client.post("/games")
    game_id = create_response.json()["game_id"]

    response = client.delete(f"/games/{game_id}")
    assert response.status_code == 200
    assert response.json()["message"] == "Game deleted"

    # Verify game is actually deleted
    get_response = client.get(f"/games/{game_id}?player=white")
    assert get_response.status_code == 404


def test_delete_nonexistent_game():
    response = client.delete("/games/nonexistent")
    assert response.status_code == 404


def test_game_flow():
    # Create game
    create_response = client.post("/games")
    game_id = create_response.json()["game_id"]

    # Make a move as white
    white_move = client.post(f"/games/{game_id}/move?player=white&move_uci=e2e4")
    assert white_move.status_code == 200

    # Check game state shows black to move
    state_response = client.get(f"/games/{game_id}?player=black")
    assert state_response.status_code == 200
    data = state_response.json()
    assert data["turn"] == "black"

    # Make a move as black
    black_move = client.post(f"/games/{game_id}/move?player=black&move_uci=e7e5")
    assert black_move.status_code == 200

    # Verify turn is back to white
    final_state = client.get(f"/games/{game_id}?player=white")
    assert final_state.status_code == 200
    assert final_state.json()["turn"] == "white"