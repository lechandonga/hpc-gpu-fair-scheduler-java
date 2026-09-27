# HPC GPU Fair Scheduler (本地模拟)

一套本地集群资源调度组件，模拟多队列 GPU 作业的提交、排队、分配、抢占与节点故障恢复。
所有调度结论都是**确定性**的：同样的输入序列必然得到同样的放置结果，且每个决策都有日志依据。

## 模块结构

```
scheduler/
├── model/               # ResourceRequest / Job / Node / QueueDef / DecisionRecord / 状态枚举
├── exception/           # SchedulingException（Reason 区分拒绝原因）
├── core/FairScheduler   # 调度核心（不依赖 Spring，可单测）
└── api/                 # Spring 装配 + REST 接口
```

## 调度策略

### 排队
- 优先级数值大者先调度。
- 同优先级按**队列权重公平**：对每个有待调度作业的队列计算 `运行中GPU数 / 权重`，
  比值最小者优先；比值相同按队列名字典序。用量为 0 的队列比值即 0，因此**不会被饿死**。
- 队列内部按提交序号（submitSeq，单调递增）FIFO。

### 放置（全有或全无）
- 作业的整份资源请求必须放进**单个节点**，不允许跨节点拆分。
- 节点按 id 字典序做 first-fit，保证顺序确定。
- 暂时放不下但理论上可满足的作业进入 PENDING 排队；超过所有节点容量的请求直接拒绝。

### 抢占
- 高优先级作业放不下时，按 `(优先级升序, 提交序号降序)` 逐个抢占低优先级 RUNNING 作业，
  每抢占一个就重试放置，**放得下就停**，不多抢。
- 被抢占作业立即释放资源并回到 PENDING（preemptionCount + 1），后续调度轮可被重新放置。
- 同优先级之间不抢占。
- 幂等：只有仍处于 RUNNING 的作业才会被抢占，重复触发同一轮调度不会重复扣减。

### 账目守恒
任何时刻满足：`Σ 各节点已分配资源 == Σ RUNNING 作业的资源请求`。
测试通过 `FairScheduler.accountingBalanced()` 在并发、抢占、故障场景后逐一核对。

## 作业状态流转

```
submit
  │
  ▼
PENDING ──放置成功──► RUNNING ──complete──► COMPLETED
  ▲                   │
  │                   ├─被更高优先级抢占──► PREEMPTED ──立即──► PENDING
  │                   │
  └────节点 DOWN 重排──┘
```

## 异常与恢复

### 提交拒绝（原因可区分）
`SchedulingException.Reason`：
| Reason | 含义 |
|---|---|
| `INVALID_REQUEST` | 资源请求为空/全零、jobId 为空 |
| `UNKNOWN_QUEUE` | 队列不存在 |
| `DUPLICATE_JOB` | 同一 jobId 重复提交（并发下恰有一个成功） |
| `INSUFFICIENT_RESOURCE` | 请求超过最大单节点容量，永远无法满足 |
| `UNKNOWN_JOB` / `UNKNOWN_NODE` | 操作对象不存在 |
| `ILLEGAL_STATE` | 状态机不允许的操作（如完成非 RUNNING 作业） |

### 节点故障
- `nodeDown(id)`：节点标记 DOWN，其上 RUNNING 作业按提交序号顺序全部重排为 PENDING，
  节点账目清零，随后触发一轮调度把能放的作业放到存活节点。**重复调用是幂等 no-op**。
- `nodeRecover(id)`：节点重新 UP，容量回到调度池，触发一轮调度。
  由于节点选择按 id 排序、作业选择按 (优先级, 队列比值, 提交序号)，恢复后的放置结果**可复现**。
- 任何时刻节点已分配量都不超过容量（不超卖），故障-恢复循环中同样成立。

### 决策日志
每个决策都追加一条 `DecisionRecord(type, detail)`，类型包括
`SUBMIT_ACCEPTED / QUEUE_SELECTED / JOB_SCHEDULED / JOB_PREEMPTED / JOB_REQUEUED /
NODE_DOWN / NODE_RECOVERED / PREEMPTION_SKIPPED` 等。
测试在每个用例结束后打印完整日志，REST 端点 `GET /api/scheduler/decisions` 也可查看。

## 本地验证

```bash
# 全部测试（并发提交 / 抢占 / 全有或全无 / 公平收敛 / 节点故障）
mvn test

# 启动本地服务（内置 2 节点 x 8 GPU，3 条权重 3:2:1 的队列）
mvn spring-boot:run

# 提交作业
curl -X POST localhost:8080/api/scheduler/jobs -H 'Content-Type: application/json' \
  -d '{"jobId":"j1","queue":"research","priority":5,"cpuCores":4,"gpus":2,"memoryMb":4096}'

# 查看作业 / 决策日志
curl localhost:8080/api/scheduler/jobs/j1
curl localhost:8080/api/scheduler/decisions

# 节点下线与恢复
curl -X POST localhost:8080/api/scheduler/nodes/node-1/down
curl -X POST localhost:8080/api/scheduler/nodes/node-1/recover
```

## 测试覆盖

| 测试类 | 场景 |
|---|---|
| `SubmissionAndPlacementTest` | 四类拒绝原因区分、排队而非拒绝、全有或全无、完成后补位 |
| `ConcurrentSubmissionTest` | 多线程提交全部接受且仅一次、并发重复提交恰一个成功、竞争下不超卖 |
| `PreemptionTest` | 受害者选择规则、抢占后重排、重复轮次不重复扣减、放得下就停、同级不抢占 |
| `FairShareTest` | 运行份额按权重收敛、小权重队列不饿死、同输入结果可复现 |
| `NodeFailureTest` | 故障重排、幂等下线、恢复后确定性重排、故障循环不超卖 |
