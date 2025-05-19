import os

output_path = "../data/processed/games_1800plus.jsonl"

print("Resolved path:", os.path.abspath(output_path))
print("Parent folder:", os.path.dirname(output_path))
print("Parent exists:", os.path.exists(os.path.dirname(output_path)))

os.makedirs(os.path.dirname(output_path), exist_ok=True)
print("✅ Directory ensured.")

with open(output_path, "w", encoding="utf-8") as f:
    f.write("Test OK\n")
print("✅ File created.")

