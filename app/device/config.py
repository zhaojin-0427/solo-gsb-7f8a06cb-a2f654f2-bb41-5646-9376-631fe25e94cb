import os

DB_PATH = os.environ.get("DEVICE_DB", "/tmp/device.db")
INITIAL_BALANCE_ML = int(os.environ.get("INITIAL_BALANCE_ML", "100"))
# 暂停等待期间，多久检查一次调用方是否已断开（断开则让出等待位，不影响其它批次）
WAIT_POLL_SECONDS = float(os.environ.get("WAIT_POLL_SECONDS", "1.0"))
