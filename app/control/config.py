import os

DB_PATH = os.environ.get("CONTROL_DB", "/tmp/control.db")
DEVICE_URL = os.environ.get("DEVICE_URL", "http://127.0.0.1:8100")
WORKER_ID = os.environ.get("WORKER_ID", "control")

# 租约：控制进程领取某批次下一步后必须在此时间内落库回执，
# 否则其它控制进程认为其失联（崩溃 / 重启 / 网络中断）并接管。
LEASE_TTL_SECONDS = float(os.environ.get("LEASE_TTL_SECONDS", "8"))
WORKER_POLL_SECONDS = float(os.environ.get("WORKER_POLL_SECONDS", "0.7"))
# 对设备请求的读超时刻意小于租约 TTL：请求挂起时本进程先放弃租约，
# 由另一个控制进程凭同一操作键重试（设备幂等，不会重复扣量）。
DEVICE_TIMEOUT_SECONDS = float(os.environ.get("DEVICE_TIMEOUT_SECONDS", "5"))
