# 加液批次实验台 (dosing-lab)

Python/FastAPI 实现的加液批次 HTTP API。一个**控制服务**（两个进程）接收加液批次并异步驱动**本地设备模拟器**；两端各自使用独立的 SQLite 数据库，全部由 Docker Compose 一键启动。

## 系统组成与端口

| 服务 | 端口 | 说明 |
|---|---|---|
| `control-1` | **8001** | 控制进程 1（接收批次、查询） |
| `control-2` | **8002** | 控制进程 2（与 control-1 对等，可处理同样的请求） |
| `device` | **9000** | 设备模拟器（A、B 两个独立通道，各初始 100 mL） |

- 两个控制进程共享**同一个**控制端 SQLite（命名卷 `control-data`，WAL 模式），通过租约表协调，保证同一批次同一时刻只有一个驱动者；进程崩溃后租约过期，另一进程接管。
- 设备模拟器使用独立的设备端 SQLite（命名卷 `device-data`）。批次、设备回执与通道余量在两端重启后均保留。
- 设备以 `<批次ID>:<步骤号>` 为操作键：同键同参数重放只返回已存回执（不重复扣量），同键不同参数返回 409；扣量/拒绝与回执在同一事务中原子落库。

## 启动

```bash
docker compose up --build
```

启动后即可访问：`http://localhost:8001`（或 8002）与 `http://localhost:9000`。

## 批次状态机

| `status` | 含义 |
|---|---|
| `pending` | 待确认（尚有步骤未确认） |
| `completed` | 已完成（全部步骤扣量成功） |
| `failed_first` | 首步失败（第一步即余量不足，未扣任何量） |
| `partial` | 部分完成（前若干步成功，后续步骤余量不足；已扣量不回滚） |

同批次步骤严格按序确认：设备回执未在控制端落库前，不会发送下一步。

## API 摘要

控制端（8001/8002）：

| 方法/路径 | 说明 |
|---|---|
| `POST /batches` | 提交批次 `{request_key, channel, steps}`。先持久化不可变批次并返回 `202` + `batch_id`，再异步驱动设备。同请求键同内容返回原批次（`200`），不同内容返回 `409`，非法输入返回 `422` |
| `GET /batches?status=...` | 列出批次（可按四种状态过滤） |
| `GET /batches/{batch_id}` | 批次详情：状态、已确认回执列表、设备实际余量 |

设备端（9000）：

| 方法/路径 | 说明 |
|---|---|
| `POST /dispense` | 执行一步扣量（控制端内部调用），键为 `{op_key: "<批次ID>:<步骤号>"}` |
| `POST /pause` | `{"channel":"A"}`：暂停 A 通道下一次成功扣量后的该操作键回执 |
| `POST /release` | `{"channel":"A"}`：手动释放被暂停的回执（`{}` 释放全部） |
| `GET /channels` | 各通道实际余量、暂停标志、被挂起的操作键 |
| `POST /reset` | 实验台复位：两通道恢复 100 mL 并清空操作日志（有挂起回执时返回 409） |

校验规则：`channel` 仅 `A`/`B`；`steps` 为 1～3 个正整数毫升步骤，单步 ≤ 100；`request_key` 非空。违反即 `422`。

## curl 演示

以下命令假设使用 `jq` 美化输出（可去掉）。先复位实验台，保证可复现：

```bash
curl -s -X POST localhost:9000/reset -H 'Content-Type: application/json' -d '{}'
```

### 1. 提交批次并查询

```bash
# 提交：B 通道顺序加液 30、20、10 mL，立即返回批次 ID
curl -s -X POST localhost:8001/batches -H 'Content-Type: application/json' \
  -d '{"request_key":"demo-1","channel":"B","steps":[30,20,10]}' | jq .

# 查询（8001/8002 均可）：状态、已确认回执、设备实际余量
BID=<上一步返回的 batch_id>
curl -s localhost:8002/batches/$BID | jq .
```

### 2. 幂等提交与冲突

```bash
# 同请求键同内容 -> 200，返回原批次（不会重复扣量）
curl -s -X POST localhost:8001/batches -H 'Content-Type: application/json' \
  -d '{"request_key":"demo-1","channel":"B","steps":[30,20,10]}' | jq .

# 同请求键不同内容 -> 409
curl -s -X POST localhost:8001/batches -H 'Content-Type: application/json' \
  -d '{"request_key":"demo-1","channel":"B","steps":[30,20]}' | jq .
```

### 3. 非法输入 -> 422

```bash
curl -s -X POST localhost:8001/batches -H 'Content-Type: application/json' \
  -d '{"request_key":"demo-bad","channel":"A","steps":[0]}' | jq .
# 其他 422 场景：步骤 >100、步骤数 0 或 4、非整数、通道非 A/B、请求键为空
```

### 4. 部分完成与首步失败

```bash
# A 有 100 mL：[60,50] -> 第一步成功，第二步余量不足 -> partial（已扣 60 不回滚）
curl -s -X POST localhost:8001/batches -H 'Content-Type: application/json' \
  -d '{"request_key":"demo-2","channel":"A","steps":[60,50]}' | jq -r .batch_id
# A 只剩 40 mL：[50] -> 首步失败 failed_first
curl -s -X POST localhost:8001/batches -H 'Content-Type: application/json' \
  -d '{"request_key":"demo-3","channel":"A","steps":[50]}' | jq -r .batch_id
```

### 5. 暂停、恢复与查询（核心演示）

```bash
curl -s -X POST localhost:9000/reset -H 'Content-Type: application/json' -d '{}'   # 复位，便于复现

# 1) 暂停 A 通道下一次成功扣量后的回执
curl -s -X POST localhost:9000/pause -H 'Content-Type: application/json' -d '{"channel":"A"}'

# 2) 提交 A 批次 [10,10]
curl -s -X POST localhost:8001/batches -H 'Content-Type: application/json' \
  -d '{"request_key":"demo-4","channel":"A","steps":[10,10]}' | jq -r .batch_id

# 3) 观察：扣量已在设备落库（A: 100->90），但回执被挂起，控制端无已确认回执
curl -s localhost:9000/channels | jq .        # held_op_keys 中可见被挂起的操作键
BID=<demo-4 的 batch_id>
curl -s localhost:8001/batches/$BID | jq .    # status=pending, confirmed_receipts=[]

# 4) 暂停期间 B 的批次仍能完成
curl -s -X POST localhost:8002/batches -H 'Content-Type: application/json' \
  -d '{"request_key":"demo-5","channel":"B","steps":[5]}' | jq -r .batch_id

# 5) 手动释放：被挂起的回执送达，控制端落库后继续下一步，批次完成（A: 90->80）
curl -s -X POST localhost:9000/release -H 'Content-Type: application/json' -d '{"channel":"A"}'
curl -s localhost:8001/batches/$BID | jq .    # status=completed，两条回执
```

### 6. 崩溃恢复（租约接管，不重复扣量）

```bash
# 暂停 A，提交批次 [8,8]，待回执被挂起后杀掉 control-1
curl -s -X POST localhost:9000/pause -H 'Content-Type: application/json' -d '{"channel":"A"}'
curl -s -X POST localhost:8001/batches -H 'Content-Type: application/json' \
  -d '{"request_key":"demo-6","channel":"A","steps":[8,8]}' | jq -r .batch_id
sleep 2
docker compose kill control-1

# 租约约 20 秒过期，control-2 自动接管：凭相同操作键向设备要回执（等待释放），不会重复扣量
sleep 25
curl -s localhost:9000/channels | jq .A       # 余量保持 72，无重复扣量

# 释放后批次由 control-2 驱动完成；重启 control-1 后两进程恢复对等
curl -s -X POST localhost:9000/release -H 'Content-Type: application/json' -d '{"channel":"A"}'
docker compose up -d control-1
```

### 7. 重启持久化

```bash
docker compose restart
# 重启后批次、设备回执与通道余量全部保留：
curl -s localhost:8001/batches | jq .
curl -s localhost:9000/channels | jq .
```

## 自动化端到端测试

`scripts/e2e_test.sh` 覆盖全部上述场景（45 项断言；含崩溃恢复共 51 项），可重复运行：

```bash
./scripts/e2e_test.sh                       # 基础全套（本机端口 8001/8002/9000）

# 含崩溃恢复段（自动 kill / 重启 control-1）：
CRASH_KILL_CMD="docker compose kill control-1" \
CRASH_START_CMD="docker compose up -d control-1" \
  ./scripts/e2e_test.sh
```

## 目录结构

```
├── docker-compose.yml      # 单条命令启动：docker compose up --build
├── control/                # 控制服务（两个进程共用镜像与 SQLite 卷）
│   └── app/{main,db,driver,schemas}.py
├── device/                 # 设备模拟器（独立 SQLite）
│   └── app/{main,db,schemas}.py
└── scripts/e2e_test.sh     # 端到端测试
```
