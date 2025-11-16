import sys
from database import reindex_db, reindex_db_continue

if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] in ("--continue", "-c", "continue"):
        print("Continuing blockchain indexing from last saved height…")
        reindex_db_continue()
    else:
        print("Reindexing blockchain database from genesis block (full rebuild)…")
        reindex_db()