# 本地集群 GPU 公平调度组件

在本地模拟多队列 GPU 作业的**提交、排队、分配、抢占与节点故障恢复**，
保证并发提交、资源抢占和节点故障场景下的调度结论**可预期、可解释、可复现**。

## 模块结构

```
scheduler/
├── model/                  # 领域模型（不可变参数 + 运行时状态）
│   ├── JobRequest          # 提交参数：jobId / queue / priority / 资源向量
│   ├── Job                 # 运行时作业：状态、所在节点、受理序号、抢占代次
│   ├── Node                # 节点：容量、已分配账目、健康状态
│   ├── ResourceSpec        # 资源向量（gpu/cpuCores/memoryMb），不可变
│   ├── JobState            # PENDING/RUNNING/PREEMPTED/COMPLETED/FAILED/REJECTED
│   ├── NodeStatus          # UP/DOWN/UNREACHABLE
│   ├── RejectionReason     # 可区分的拒绝原因（见下）
│   └── SubmitResult        # 提交结果：受理 或 带原因的拒绝
├── core/
│   ├── Scheduler           # 调度器核心（所有状态变更集中在一把锁内）
│   ├── FairSharePolicy     # 加权公平份额（最大剩余法取整，确定性）
│   └── DecisionLog         # 决策日志：每次决策的动作、作业、依据
└── api/SchedulerController # REST 入口（本地手动验证用）
```

## 调度策略

1. **提交校验（拒绝原因可区分）**
   - `INVALID_REQUEST`：id/队列名为空、资源非正、或请求超过任何单节点容量
     （全有或全无放置永远无法满足）；
   - `UNKNOWN_QUEUE`：目标队列未注册；
   - `DUPLICATE_JOB`：相同 jobId 已受理（幂等去重，并发下同 id 恰好受理一次）；
   - `INSUFFICIENT_RESOURCES`：即使抢占掉全集群运行作业也放不下。
   通过校验的作业进入队列；暂时资源不足但未来可满足的作业**排队等待**而非被拒。
2. **排队次序（确定性）**：欠账队列（用量 < 公平份额）优先 → 优先级高优先 →
   受理序号小优先。与线程调度顺序无关。
3. **全有或全无放置**：按 nodeId 字典序 first-fit，作业整体落在单个节点，
   任何维度（GPU/CPU/内存）不足都不得部分放置、不得跨节点拆分。
4. **公平份额与抢占**：
   - 各队列公平份额 = 按权重对 UP 节点 GPU 总量做最大剩余法取整（份额之和严格等于总量）；
   - 受害者资格：优先级严格更低；**或**候选队列欠账且受害者队列超出份额、优先级不更高；
   - 受害者按（优先级升序、受理序号降序）逐个抢占，恰好抢到候选能放下为止；
   - 欠账队列因此获得资源，多队列共享时用量向权重比例收敛，后排队列不被饿死。
5. **抢占幂等**：抢占是状态守卫操作（仅 `RUNNING → PREEMPTED` 时释放一次资源，
   并递增抢占代次 epoch）；重复触发同一次抢占为无操作，
   不会重复扣减、不会重复调度。节点 `release` 额外做了下限钳制双保险。

## 作业状态流转

```
submit ──校验失败──> REJECTED（未入队，带 RejectionReason）
submit ──校验通过──> PENDING ──放置成功──> RUNNING ──complete──> COMPLETED
                      ^  │                    │
                      │  └── 暂无资源则排队    ├── 被抢占 ──> PREEMPTED ──重排──> PENDING
                      └──────────────────────┴── 节点故障 ──> PENDING（重排）
```

## 节点异常与恢复

- **DOWN / UNREACHABLE**：节点上全部 RUNNING 作业按受理序号确定性地逐出，
  状态回到 PENDING、节点账目一次性清零（释放与重排一一对应，杜绝同一份资源
  被两个作业占用的超卖）；随后立即在存活节点上重排，容量不足的作业明确保持排队。
- **恢复 UP**：节点重新参与分配，排队作业被确定性地补调度。
- 所有迭代基于排序结构（节点按 nodeId、队列按名称、作业按受理序号），
  相同事件序列重放必然得到相同放置结论（测试 `recoveryReschedulingIsReproducible` 验证）。

## 账目守恒不变式

`Scheduler.accountingConsistent()` 在每次测试断言中使用：

1. 每个节点 `allocated ≤ capacity`；
2. 全部节点 allocated 之和 == 全部 RUNNING 作业需求之和；
3. 每个 RUNNING 作业都落在某个 UP 节点上（无悬空、无超卖）。

## 本地验证

```bash
# 全部测试（并发提交 / 抢占 / 全有或全无 / 节点故障）
mvn test

# 单独跑某类场景，日志会打印每个决策的依据（[DecisionLog] 行）
mvn test -Dtest=PreemptionTest
mvn test -Dtest=NodeFailureTest

# 启动 REST 服务手动验证
mvn spring-boot:run
curl -X POST localhost:8080/api/scheduler/jobs \
  -H 'Content-Type: application/json' \
  -d '{"jobId":"j1","queue":"alpha","priority":5,
       "resources":{"gpu":2,"cpuCores":4,"memoryMb":8000}}'
```

测试日志中 `[DecisionLog] [ACTION] job=xxx :: 依据` 记录每次
受理/拒绝/放置/抢占/逐出/等待的具体原因，便于排查。
