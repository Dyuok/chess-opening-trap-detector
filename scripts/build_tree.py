import json
import chess
import pickle

def load_games(jsonl_path):
    """Lädt alle Spiele aus einer JSONL-Datei."""
    with open(jsonl_path, "r", encoding="utf-8") as f:
        for line in f:
            yield json.loads(line)

def update_tree(tree, fen_index, moves, result, white_elo, black_elo, max_depth=30):
    """Fügt eine Zugfolge dem Baum hinzu und aktualisiert Statistiken."""
    node = tree
    depth = 0
    board = chess.Board()

    for move in moves:
        if depth >= max_depth:
            break

        if move not in node:
            fen = board.fen()
            node[move] = {
                "count": 0,
                "white_wins": 0,
                "black_wins": 0,
                "draws": 0,
                "total_white_elo": 0,
                "total_black_elo": 0,
                "fen": fen,
                "eval_cp": None,           # Best centipawn eval from Lichess
                "only_move_gap": None,      # Difference to second-best move (if available)
                "children": {}
            }
            fen_index[fen] = node[move]  # ✅ store reference

        node = node[move]
        node["count"] += 1
        node["total_white_elo"] += int(white_elo)
        node["total_black_elo"] += int(black_elo)
        if result == "1-0":
            node["white_wins"] += 1
        elif result == "0-1":
            node["black_wins"] += 1
        elif result == "1/2-1/2":
            node["draws"] += 1

        board.push_uci(move) 
        node = node["children"]
        depth += 1

if __name__ == "__main__":
    path = "H:/Coding_Projects/opening_traps/chess-opening-trap-detector/data/processed/games_1800plus_test.jsonl"
    fen_index = {}
    tree = {}

    for i, game in enumerate(load_games(path)):
        update_tree(
            tree,
            fen_index,
            moves=game["moves"],
            result=game["result"],
            white_elo=game["white_elo"],
            black_elo=game["black_elo"]
        )

        if i % 1000 == 0:
            print(f"{i} games processed...")

        if i % 10000 == 0 and i > 0:
            print(f"{i} games processed... dumping partial tree...")
            with open("H:/Coding_Projects/opening_traps/chess-opening-trap-detector/data/processed/opening_tree_test_partial.pkl", "wb") as f_partial:
                pickle.dump(tree, f_partial, protocol=pickle.HIGHEST_PROTOCOL)

    # Save the tree
    with open("H:/Coding_Projects/opening_traps/chess-opening-trap-detector/data/processed/opening_tree_test_partial.pkl", "wb") as f_out:
        pickle.dump(tree, f_out, protocol=pickle.HIGHEST_PROTOCOL)
        
    print("✅ Opening tree saved.")