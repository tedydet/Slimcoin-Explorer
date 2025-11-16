import time
from database import update_with_latest_block, update_peers
print("Updating database...")

while True:
    update_peers()
    update_with_latest_block()
    time.sleep(60)
