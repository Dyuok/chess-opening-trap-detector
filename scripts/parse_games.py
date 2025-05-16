import zstandard as zstd
import chess.pgn
import os
import io
import json

def stream_games_from_zst(file_path):
    """Liest PGN-Spiele aus einer .zst-Datei im Stream."""
    with open(file_path, 'rb') as compressed:
        dctx = zstd.ZstdDecompressor()
        stream_reader = dctx.stream_reader(compressed)
        text_stream = io.TextIOWrapper(stream_reader, encoding='utf-8')

        while True:
            game = chess.pgn.read_game(text_stream)
            if game is None:
                break
            yield game

def extract_game_data(game):
    """Extrahiert relevante Informationen aus einem PGN-Spiel."""
    headers = game.headers
    game_id = headers.get("Site", "").split("/")[-1]
    white_elo = headers.get("WhiteElo")
    black_elo = headers.get("BlackElo")
    eco = headers.get("ECO")
    result = headers.get("Result")

    # SAN-Zugliste
    board = game.board()
    moves = []
    for move in game.mainline_moves():
        moves.append(board.san(move))
        board.push(move)

    return {
        "id": game_id,
        "white_elo": white_elo,
        "black_elo": black_elo,
        "eco": eco,
        "result": result,
        "moves": moves
    }

if __name__ == "__main__":
    path_to_zst = "data/raw/lichess_db_standard_rated_2025-04.pgn.zst"  # anpassen
    # output_path = "../data/processed/games_1800plus.jsonl"
    output_path = "H:/Coding_Projects/opening_traps/chess-opening-trap-detector/data/processed/games_1800plus.jsonl"


    print("Lade Spiele mit Elo ≥ 1800 und speichere sie...")

    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    # print("Resolved path:", os.path.abspath(output_path))
    # print("Parent directory:", os.path.dirname(output_path))
    # print("Parent exists:", os.path.exists(os.path.dirname(output_path)))print("Resolved path:", os.path.abspath(output_path))
    # print("Parent folder exists:", os.path.exists(os.path.dirname(output_path)))

    with open(output_path, "w", encoding="utf-8") as f_out:
        count = 0
        for game in stream_games_from_zst(path_to_zst):
            data = extract_game_data(game)

            try:
                if int(data["white_elo"]) < 1800 or int(data["black_elo"]) < 1800:
                    continue
            except (TypeError, ValueError):
                continue

            f_out.write(json.dumps(data) + "\n")
            count += 1
            if count % 1000 == 0:
                print(f"{count} Spiele gespeichert...")

    print(f"✅ Fertig: {count} Spiele gespeichert in {output_path}")