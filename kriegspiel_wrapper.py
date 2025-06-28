"""
Wrapper for the BerkeleyGame class to add additional functionality
without modifying the original ks-game code.
"""

import chess
from kriegspiel.berkeley import BerkeleyGame


class ExtendedBerkeleyGame:
    """
    Wrapper around BerkeleyGame that adds additional methods
    for API integration while keeping the original code unchanged.
    """

    def __init__(self, any_rule=True):
        """Initialize with an underlying BerkeleyGame instance."""
        self._game = BerkeleyGame(any_rule=any_rule)

    def get_visible_board(self, color):
        """
        Get the board state visible to a specific player.

        In Kriegspiel, players can only see their own pieces and cannot see
        opponent pieces. This method returns a board with only the pieces
        belonging to the specified color.

        Args:
            color: Chess color (chess.WHITE or chess.BLACK) for which to get visible board

        Returns:
            chess.Board: A board copy showing only pieces belonging to the specified color
        """
        # Create a copy of the referee's board
        visible_board = self._game._board.copy(stack=False)

        # Remove all pieces that don't belong to the specified color
        for square in chess.SQUARES:
            piece = visible_board.piece_at(square)
            if piece is not None and piece.color != color:
                visible_board.remove_piece_at(square)

        return visible_board

    # Delegate all other methods to the underlying game
    def __getattr__(self, name):
        """Delegate attribute access to the underlying BerkeleyGame instance."""
        return getattr(self._game, name)
