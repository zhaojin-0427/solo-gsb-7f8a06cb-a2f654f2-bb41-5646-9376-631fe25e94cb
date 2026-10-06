# 实验台加液批次 HTTP API

Python / FastAPI 实现。三个容器由 Docker Compose 同时启动：

| 服务 | 角色 | 宿主端口 | 独立 SQLite |
| --- | --- | --- | --- |
| `device` | 本地设备模拟器（A/B 两通道，各 100 mL） | **8100** | `/data/device.db` |
| `control1` | 控制服务进程 1（异步驱动 worker） | **8001** | `/data/control.db`（共享卷） |
| `control2` | 控制服务进程 2（异步驱动 worker） | **8002** | `/data/control.db`（共享卷） |

两个控制进程共用同一个控制端数据库文件（WAL 模式，跨进程互斥），
设备模拟器使用**完全独立**的另一个数据库。三者均挂载 Docker 卷，重启后
批次、设备回执与余量全部保留。

## 一条命令启动

```bash
docker compose up --build -d
```

查看状态：

```bash
docker compose ps
docker compose logs -f device control1 control2
```

停止（保留数据）/ 彻底重置：

```bash
docker compose down            # 保留卷，重启后数据仍在
docker compose down -v         # 删除卷，恢复到初始 100/100 mL
```

> 没有 Docker 的环境也可以直接跑（需 Python 3.11 与 `requirements.txt` 依赖）：
> 先启动 `uvicorn app.device.main:app`，再以不同 `WORKER_ID` 启动两个
> `uvicorn app.control.main:app`，分别监听 8100 / 8001 / 8002。

## 快速演示：提交 → 暂停 A → 查询 → B 照常完成 → 恢复 → 再查询

控制端两个端口（8001/8002）完全等价，可随便切换（示例统一用 8001）。

### 1. 提交一个 A 通道批次（1～3 个顺序正整数毫升步骤，单步 ≤ 100 mL）

```bash
curl -s -X POST http://localhost:8001/batches \
  -H 'Content-Type: application/json' \
  -d '{"request_key":"demo-1","channel":"A","steps":[20,30,10]}'
# -> 202 立即返回 {"batch_id":"...", "reused": false, ...}
```

控制端先把**不可变批次**落库并立即返回批次 ID，再由后台 worker 异步驱动设备。
用返回的 `batch_id` 查询（也可以走 8002，验证两个进程数据一致）：

```bash
BID=<上一步返回的 batch_id>
curl -s http://localhost:8002/batches/$BID
```

### 2. 暂停：布防一次性暂停臂，A 的“下一次成功扣量”回执被挂起

```bash
curl -s -X POST http://localhost:8100/admin/pause/A
```

暂停只影响 **A 通道下一次成功扣量的那一个操作键**（扣量本身已原子落库，
只是回执暂不返回），一次性、触发即解除。

```bash
curl -s -X POST http://localhost:8001/batches \
  -H 'Content-Type: application/json' \
  -d '{"request_key":"demo-hold","channel":"A","steps":[40,5]}'
# 记下 batch_id
HID=<返回的 batch_id>
```

此时：设备 A 已扣 40 mL（`GET :8100/balances` 可见），但控制端尚未拿到回执，
批次查询显示 **`pending`（待确认）、已确认 0 步**；重复提交 / 重试只会等待，
**不会重复扣量**：

```bash
curl -s http://localhost:8100/balances           # {"A":40,...} 扣量已发生
curl -s http://localhost:8001/batches/$HID       # status=pending, confirmed_steps=0
curl -s http://localhost:8100/admin/state        # held_in_memory 列出挂起键
```

### 3. 暂停期间 B 的批次照常完成

```bash
curl -s -X POST http://localhost:8001/batches \
  -H 'Content-Type: application/json' \
  -d '{"request_key":"demo-b","channel":"B","steps":[10,10,10]}'
# 记为 BBID，几秒后查询为 completed，B=70，不受 A 暂停影响
```

### 4. 手动恢复挂起的操作键

```bash
curl -s -X POST http://localhost:8100/admin/resume/$HID:1
```

`:1` 是被挂起的操作键（操作键 = `批次ID:步骤号`）。释放后控制端立即拿到
**原来那张回执**并继续顺序执行第 2 步：

```bash
sleep 1
curl -s http://localhost:8001/batches/$HID
# status=completed, confirmed_steps=2，receipts 中第1步 remaining_ml_after=40
```

### 5. 列表查询（区分四种状态，附已确认回执与设备实际余量）

```bash
curl -s http://localhost:8001/batches                 # 全部，含各状态计数
curl -s 'http://localhost:8001/batches?status=pending'              # 待确认
curl -s 'http://localhost:8001/batches?status=completed'            # 已完成
curl -s 'http://localhost:8001/batches?status=first_step_failed'    # 首步失败
curl -s 'http://localhost:8001/batches?status=partial'              # 部分完成
```

状态语义（恰好四种，已确认回执在任何状态下都逐条列出）：

- `pending` 待确认：尚有步骤未确认（包括还没有任何回执，以及前几步已确认、
  后续仍在执行 / 等待租约接管）；
- `completed` 已完成：全部步骤成功；
- `first_step_failed` 首步失败：第 1 步即被设备以余量不足拒绝；
- `partial` 部分完成：已有成功扣量，随后某步余量不足被拒，**停止后续步骤且不回滚**。

### 6. 余量不足：停止后续、不回滚

当前 A 余量为 5（35 已扣），提交 `[5,10]`：

```bash
curl -s -X POST http://localhost:8001/batches \
  -H 'Content-Type: application/json' \
  -d '{"request_key":"demo-partial","channel":"A","steps":[5,10]}'
# -> status=partial：第1步 success（A=0），第2步 rejected(insufficient_balance)，
#    第1步扣量不回滚，且不会再有后续步骤
```

## 非法输入 → 422

缺字段、通道非 A/B、步骤数不是 1～3、非正整数、布尔、单步 > 100 mL 等，
Pydantic 统一返回 422：

```bash
curl -i -X POST http://localhost:8001/batches -H 'Content-Type: application/json' \
  -d '{"request_key":"x","channel":"A","steps":[0]}'           # 422
curl -i -X POST http://localhost:8001/batches -H 'Content-Type: application/json' \
  -d '{"request_key":"x","channel":"A","steps":[101]}'         # 422
curl -i -X POST http://localhost:8001/batches -H 'Content-Type: application/json' \
  -d '{"request_key":"x","channel":"A","steps":[1,2,3,4]}'     # 422
```

## 请求键幂等与 409

- 同一 `request_key` + **完全相同内容**（通道、步骤）→ 返回原批次（`reused:true`），不重复执行；
- 同一 `request_key` + **内容不同** → **409**，并返回已存在的批次 ID。

```bash
curl -s -X POST http://localhost:8001/batches -H 'Content-Type: application/json' \
  -d '{"request_key":"k","channel":"B","steps":[5]}'           # 202 新建
curl -s -X POST http://localhost:8002/batches -H 'Content-Type: application/json' \
  -d '{"request_key":"k","channel":"B","steps":[5]}'           # 202 reused:true
curl -i -X POST http://localhost:8001/batches -H 'Content-Type: application/json' \
  -d '{"request_key":"k","channel":"B","steps":[6]}'           # 409
```

设备端同样保证：同一操作键 + 同参数永远只重放同一张回执；
同键不同参数返回 **409**。

## 崩溃 / 重启 / 租约过期恢复（不重复扣量）

- 操作键为 `批次ID:步骤号`；worker 每领取一批次的下一步都带 **8 秒租约**（`LEASE_TTL_SECONDS`）。
- worker 在“设备已扣量但回执未落控制端库”时崩溃 / 被 kill / 网络挂起：
  租约过期后**另一个控制进程**凭**同一个操作键**重试，设备幂等重放原回执，
  随后继续未执行步骤。可复现：

  ```bash
  # 1) 暂停 A 后提交一个多步批次，使其卡在第1步
  curl -s -X POST http://localhost:8100/admin/pause/A
  curl -s -X POST http://localhost:8001/batches -H 'Content-Type: application/json' \
    -d '{"request_key":"crash","channel":"A","steps":[10,10,10]}'
  # 2) 直接 kill 持租约的那个控制进程（看容器日志或 work_state 表可知持有者）
  docker compose kill control1
  # 3) 等 ~10 秒让 control2 接管，然后释放；只会看到同一张回执、3 条设备操作
  curl -s -X POST http://localhost:8100/admin/resume/<batch_id>:1
  docker compose start control1
  ```

- 设备进程在挂起期间重启：重启前遗留的挂起记录会在控制端凭同键重试时
  自动幂等重放（扣量事实保留），无需人工干预；批次、回执、余量均从 SQLite 恢复。
- 想整体验证重启保留：`docker compose restart`，然后再 `GET /batches`、`GET :8100/balances`。

## HTTP 接口一览

控制服务（8001 / 8002）：

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/batches` | 提交批次，202 立即返回 `batch_id`；同键同内容复用、异内容 409 |
| GET | `/batches?status=...` | 列表 + 状态计数 + 设备余量 |
| GET | `/batches/{batch_id}` | 状态、已确认回执、该通道设备实际余量 |
| GET | `/balances` | 设备 A/B 实际余量（透传设备） |
| GET | `/healthz` | 健康检查（返回自身 worker_id） |

设备模拟器（8100）：

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/dispense` | 以 `op_key` 幂等的加液：成功扣量 / 余量不足拒绝原子落库；同键异参 409 |
| GET | `/balances` | A/B 实际余量 |
| GET | `/operations?channel=A` | 设备侧全部持久化操作与回执 |
| GET | `/operations/{op_key}` | 查单个操作键 |
| POST | `/admin/pause/{channel}` | 一次性暂停臂 |
| POST | `/admin/resume/{op_key}` | 手动释放挂起回执 |
| DELETE | `/admin/pause` | 撤销尚未触发的暂停臂 |
| GET | `/admin/state` | 暂停臂与内存挂起键 |
| GET | `/healthz` | 健康检查 |

## 关键设计

- **不可变批次先行**：`POST /batches` 在一个 `BEGIN IMMEDIATE` 事务内完成
  请求键查重与批次插入，然后立即返回；驱动全部在后台 worker。
- **严格顺序 + 回执先落库**：worker 按已确认回执数推导下一步，回执落库与
  租约释放在同一事务；第 N 步回执未在控制端落库前绝不发送第 N+1 步。
- **双层幂等**：控制端 `request_key` 唯一（同内容返回原批次，异内容 409）；
  设备端 `op_key=batch_id:step_no` 唯一（同参重放回执，异参 409）。
- **租约接管**：两个控制进程共享控制库，`work_state` 行锁 + TTL；
  设备 HTTP 读超时（5s）小于租约 TTL（8s），失联后由对端安全接管。
- **原子扣量**：设备端“读余额—扣减/拒绝—写回执—消费暂停臂”在单个
  SQLite 写事务内完成，A/B 通道仅在各自行上竞争。
