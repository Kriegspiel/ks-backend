import sys
import os
import uuid
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'ks-game'))

from fastapi import FastAPI, HTTPException
from kriegspiel.berkeley import BerkeleyGame
from kriegspiel.move import KriegspielMove, QuestionAnnouncement
import chess

app = FastAPI(title="Kriegspiel Chess API", description="API for playing Kriegspiel chess")

games = {}

@app.get("/")
async def root():
    return {"message": "Kriegspiel Chess API"}

@app.post("/games")
async def create_game(any_rule: bool = True):
    game_id = str(uuid.uuid4())
    games[game_id] = BerkeleyGame(any_rule=any_rule)
    return {"game_id": game_id, "status": "created", "any_rule": any_rule}

@app.get("/games/{game_id}")
async def get_game_state(game_id: str, player: str):
    if game_id not in games:
        raise HTTPException(status_code=404, detail="Game not found")
    
    game = games[game_id]
    if player not in ["white", "black"]:
        raise HTTPException(status_code=400, detail="Player must be 'white' or 'black'")
    
    color = chess.WHITE if player == "white" else chess.BLACK
    visible_board = game.get_visible_board(color)
    
    return {
        "game_id": game_id,
        "player": player,
        "visible_board": str(visible_board),
        "turn": "white" if game.board.turn == chess.WHITE else "black",
        "is_game_over": game.board.is_game_over()
    }

@app.post("/games/{game_id}/move")
async def make_move(game_id: str, player: str, move_uci: str, question_type: str = "COMMON"):
    if game_id not in games:
        raise HTTPException(status_code=404, detail="Game not found")
    
    game = games[game_id]
    if player not in ["white", "black"]:
        raise HTTPException(status_code=400, detail="Player must be 'white' or 'black'")
    
    color = chess.WHITE if player == "white" else chess.BLACK
    
    if game.board.turn != color:
        raise HTTPException(status_code=400, detail="Not your turn")
    
    try:
        move = chess.Move.from_uci(move_uci)
        question = QuestionAnnouncement.COMMON if question_type == "COMMON" else QuestionAnnouncement.ASK_ANY
        ks_move = KriegspielMove(question, move)
        
        answer = game.ask_for(ks_move, color)
        
        return {
            "game_id": game_id,
            "player": player,
            "move": move_uci,
            "legal": answer.is_legal,
            "announcement": answer.announcement.name if answer.announcement else None,
            "special_case": answer.special_case.name if answer.special_case else None,
            "visible_board": str(game.get_visible_board(color)),
            "is_game_over": game.board.is_game_over()
        }
    except ValueError as e:
        raise HTTPException(status_code=400, detail=f"Invalid move: {str(e)}")

@app.delete("/games/{game_id}")
async def delete_game(game_id: str):
    if game_id not in games:
        raise HTTPException(status_code=404, detail="Game not found")
    del games[game_id]
    return {"message": "Game deleted"}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
