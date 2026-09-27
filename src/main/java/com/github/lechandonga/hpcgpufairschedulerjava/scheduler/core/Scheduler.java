package com.github.lechandonga.hpcgpufairschedulerjava.scheduler.core;

import com.github.lechandonga.hpcgpufairschedulerjava.scheduler.model.Job;
import com.github.lechandonga.hpcgpufairschedulerjava.scheduler.model.JobRequest;
import com.github.lechandonga.hpcgpufairschedulerjava.scheduler.model.JobState;
import com.github.lechandonga.hpcgpufairschedulerjava.scheduler.model.Node;
import com.github.lechandonga.hpcgpufairschedulerjava.scheduler.model.NodeStatus;
import com.github.lechandonga.hpcgpufairschedulerjava.scheduler.model.RejectionReason;
import com.github.lechandonga.hpcgpufairschedulerjava.scheduler.model.ResourceSpec;
import com.github.lechandonga.hpcgpufairschedulerjava.scheduler.model.SubmitResult;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;

/**
 * 本地集群调度器：多队列 GPU 作业的提交、排队、分配、抢占与节点故障恢复。
 *
 * 设计要点：
 * - 所有状态变更都在本对象的内部锁内完成，并发提交下账目守恒；
 * - 所有迭代基于排序后的结构（节点按 nodeId、队列按名称、作业按受理序号），
 *   调度结论与触发顺序无关，可复现；
 * - 抢占/释放均为状态守卫的幂等操作，重复触发同一次抢占不会重复扣减。
 */
public final class Scheduler {

    private final Object lock = new Object();

    /** 队列名 -> 权重（按名称排序，保证确定性）。 */
    private final Map<String, Double> queueWeights = new TreeMap<>();
    /** 节点表（按 nodeId 排序）。 */
    private final Map<String, Node> nodes = new TreeMap<>();
    /** 已受理作业（jobId -> Job），受理顺序即入队顺序。 */
    private final Map<String, Job> jobs = new LinkedHashMap<>();

    private final FairSharePolicy fairShare = new FairSharePolicy();
    private final DecisionLog decisionLog = new DecisionLog();

    public void addQueue(String name, double weight) {
        synchronized (lock) {
            queueWeights.put(name, weight);
        }
    }

    public void addNode(Node node) {
        synchronized (lock) {
            nodes.put(node.nodeId(), node);
        }
    }

    /** 提交作业：校验 -> 去重 -> 入队 -> 尝试调度。拒绝原因彼此可区分。 */
    public SubmitResult submit(JobRequest request) {
        synchronized (lock) {
            SubmitResult validation = validate(request);
            if (validation != null) {
                decisionLog.log("REJECT", request == null ? "null" : request.jobId(), validation.detail());
                return validation;
            }
            Job job = new Job(request);
            jobs.put(job.jobId(), job);
            decisionLog.log("ACCEPT", job.jobId(),
                    "queued: queue=%s priority=%d req=%s".formatted(job.queue(), job.priority(), job.resources()));
            scheduleOnceLocked();
            return SubmitResult.ok();
        }
    }

    /** 返回拒绝结果；合法则返回 null。 */
    private SubmitResult validate(JobRequest r) {
        if (r == null || r.jobId() == null || r.jobId().isBlank()
                || r.queue() == null || r.queue().isBlank()
                || r.resources() == null || !r.resources().isPositive()) {
            return SubmitResult.rejected(RejectionReason.INVALID_REQUEST,
                    "invalid request: blank id/queue or non-positive resources");
        }
        if (!queueWeights.containsKey(r.queue())) {
            return SubmitResult.rejected(RejectionReason.UNKNOWN_QUEUE,
                    "unknown queue: " + r.queue());
        }
        if (jobs.containsKey(r.jobId())) {
            return SubmitResult.rejected(RejectionReason.DUPLICATE_JOB,
                    "duplicate jobId: " + r.jobId());
        }
        // 全有或全无：单节点容量都放不下的请求永远无法放置 -> 参数层面非法
        boolean fitsSomeNode = nodes.values().stream()
                .anyMatch(n -> r.resources().fitsWithin(n.capacity()));
        if (!fitsSomeNode) {
            return SubmitResult.rejected(RejectionReason.INVALID_REQUEST,
                    "request exceeds every single node capacity (all-or-nothing unsatisfiable)");
        }
        // 即使抢占掉集群全部运行作业也放不下 -> 资源不足
        ResourceSpec total = totalCapacity();
        if (!r.resources().fitsWithin(total)) {
            return SubmitResult.rejected(RejectionReason.INSUFFICIENT_RESOURCES,
                    "request exceeds total cluster capacity " + total);
        }
        return null;
    }

    /** 执行一轮调度（公开入口，自带锁）。 */
    public void scheduleOnce() {
        synchronized (lock) {
            scheduleOnceLocked();
        }
    }

    /**
     * 一轮调度：按确定性顺序扫描排队作业，逐个尝试全有或全无放置；
     * 放不下时按规则抢占。每个作业每轮最多尝试一次，循环必然终止。
     */
    private void scheduleOnceLocked() {
        List<Job> pending = sortedPending();
        for (Job candidate : pending) {
            if (candidate.state() != JobState.PENDING && candidate.state() != JobState.PREEMPTED) {
                continue;
            }
            if (tryPlace(candidate)) {
                continue;
            }
            tryPreemptAndPlace(candidate);
        }
    }

    /** 排队作业排序：欠账队列优先 -> 优先级高优先 -> 受理序号小优先。 */
    private List<Job> sortedPending() {
        Map<String, Integer> usage = usageByQueue();
        int totalGpu = totalCapacity().gpu();
        List<Job> pending = jobs.values().stream()
                .filter(j -> j.state() == JobState.PENDING || j.state() == JobState.PREEMPTED)
                .sorted(Comparator
                        .comparing((Job j) -> !fairShare.isStarving(j.queue(), queueWeights, usage, totalGpu))
                        .thenComparing(Comparator.comparingInt(Job::priority).reversed())
                        .thenComparingLong(Job::sequence))
                .toList();
        return new ArrayList<>(pending);
    }

    /** 全有或全无放置：按 nodeId 字典序首个放得下的 UP 节点。 */
    private boolean tryPlace(Job job) {
        for (Node node : nodes.values()) {
            if (node.tryAllocate(job.resources())) {
                job.nodeId(node.nodeId());
                job.state(JobState.RUNNING);
                decisionLog.log("PLACE", job.jobId(),
                        "placed on node=%s req=%s (first-fit by nodeId, all-or-nothing)"
                                .formatted(node.nodeId(), job.resources()));
                return true;
            }
        }
        return false;
    }

    /**
     * 为候选作业执行抢占后重试放置。
     * 受害者资格：优先级严格更低；或候选队列欠账且受害者队列超出其公平份额、优先级不更高。
     * 受害者按（优先级升序、受理序号降序）确定性地逐个抢占，直到候选能放下或无可抢占。
     */
    private void tryPreemptAndPlace(Job candidate) {
        Map<String, Integer> usage = usageByQueue();
        int totalGpu = totalCapacity().gpu();
        boolean candidateStarving = fairShare.isStarving(candidate.queue(), queueWeights, usage, totalGpu);
        Map<String, Integer> shares = fairShare.fairShares(queueWeights, totalGpu);

        List<Job> victims = jobs.values().stream()
                .filter(j -> j.state() == JobState.RUNNING)
                .filter(j -> j.priority() < candidate.priority()
                        || (candidateStarving && j.priority() <= candidate.priority()
                            && usage.getOrDefault(j.queue(), 0) > shares.getOrDefault(j.queue(), 0)))
                .sorted(Comparator.comparingInt(Job::priority)
                        .thenComparing(Comparator.comparingLong(Job::sequence).reversed()))
                .toList();

        for (Job victim : victims) {
            preempt(victim, "preempted by job=%s (queue=%s starving=%s)"
                    .formatted(candidate.jobId(), candidate.queue(), candidateStarving));
            if (tryPlace(candidate)) {
                return;
            }
        }
        decisionLog.log("WAIT", candidate.jobId(),
                "no fit and no more eligible victims; stays queued");
    }

    /**
     * 抢占一个运行中作业：状态守卫，仅 RUNNING -> PREEMPTED 时释放一次资源。
     * 重复触发同一抢占为无操作，不会重复扣减。
     */
    private void preempt(Job victim, String why) {
        if (victim.state() != JobState.RUNNING) {
            decisionLog.log("PREEMPT-SKIP", victim.jobId(),
                    "already %s, duplicate preemption ignored".formatted(victim.state()));
            return;
        }
        Node node = nodes.get(victim.nodeId());
        if (node != null) {
            node.release(victim.resources());
        }
        victim.nodeId(null);
        victim.state(JobState.PREEMPTED);
        victim.preemptionEpoch(victim.preemptionEpoch() + 1);
        decisionLog.log("PREEMPT", victim.jobId(),
                "%s; resources released once, epoch=%d, back to queue"
                        .formatted(why, victim.preemptionEpoch()));
    }

    /** 节点健康变化：下线/失联时处置其上作业（重排），恢复后重新调度。 */
    public void onNodeStatusChange(String nodeId, NodeStatus status) {
        synchronized (lock) {
            Node node = nodes.get(nodeId);
            if (node == null || node.status() == status) {
                return;
            }
            node.status(status);
            if (status == NodeStatus.UP) {
                decisionLog.log("NODE-UP", "-", "node=%s recovered, rescheduling".formatted(nodeId));
            } else {
                // 故障/下线：节点上运行作业统一重排，资源一次性回收，杜绝超卖
                List<Job> affected = jobs.values().stream()
                        .filter(j -> j.state() == JobState.RUNNING && nodeId.equals(j.nodeId()))
                        .sorted(Comparator.comparingLong(Job::sequence))
                        .toList();
                for (Job job : affected) {
                    job.nodeId(null);
                    job.state(JobState.PENDING);
                    decisionLog.log("EVICT", job.jobId(),
                            "node=%s -> %s, job requeued for deterministic reschedule"
                                    .formatted(nodeId, status));
                }
                node.release(node.allocated()); // 账目清零，释放与重排一一对应
            }
            scheduleOnceLocked();
        }
    }

    /** 标记作业完成，释放资源并触发重排。 */
    public boolean complete(String jobId) {
        synchronized (lock) {
            Job job = jobs.get(jobId);
            if (job == null || job.state() != JobState.RUNNING) {
                return false;
            }
            Node node = nodes.get(job.nodeId());
            if (node != null) {
                node.release(job.resources());
            }
            job.nodeId(null);
            job.state(JobState.COMPLETED);
            decisionLog.log("COMPLETE", jobId, "finished, resources released");
            scheduleOnceLocked();
            return true;
        }
    }

    // ---------- 查询与账目 ----------

    public Optional<Job> job(String jobId) {
        synchronized (lock) {
            return Optional.ofNullable(jobs.get(jobId));
        }
    }

    public List<Job> jobsInState(JobState state) {
        synchronized (lock) {
            return jobs.values().stream().filter(j -> j.state() == state).toList();
        }
    }

    public Map<String, Node> nodes() {
        synchronized (lock) {
            return Map.copyOf(nodes);
        }
    }

    public DecisionLog decisionLog() {
        return decisionLog;
    }

    public Map<String, Integer> usageByQueue() {
        synchronized (lock) {
            Map<String, Integer> usage = new TreeMap<>();
            for (Job j : jobs.values()) {
                if (j.state() == JobState.RUNNING) {
                    usage.merge(j.queue(), j.resources().gpu(), Integer::sum);
                }
            }
            return usage;
        }
    }

    public Map<String, Integer> fairShares() {
        synchronized (lock) {
            return fairShare.fairShares(queueWeights, totalCapacity().gpu());
        }
    }

    /** 仅统计 UP 节点容量：下线/失联节点的资源不参与分配与公平份额。 */
    private ResourceSpec totalCapacity() {
        ResourceSpec total = ResourceSpec.ZERO;
        for (Node n : nodes.values()) {
            if (n.status() == NodeStatus.UP) {
                total = total.plus(n.capacity());
            }
        }
        return total;
    }

    /**
     * 资源账目守恒校验：
     * 1) 每个节点 allocated 不超过 capacity；
     * 2) 全部节点 allocated 之和 == 全部 RUNNING 作业需求之和；
     * 3) 每个 RUNNING 作业落在某个 UP 节点上（无超卖、无悬空）。
     */
    public boolean accountingConsistent() {
        synchronized (lock) {
            ResourceSpec allocatedSum = ResourceSpec.ZERO;
            for (Node n : nodes.values()) {
                ResourceSpec a = n.allocated();
                if (!a.fitsWithin(n.capacity())) {
                    return false;
                }
                allocatedSum = allocatedSum.plus(a);
            }
            ResourceSpec runningSum = ResourceSpec.ZERO;
            for (Job j : jobs.values()) {
                if (j.state() == JobState.RUNNING) {
                    runningSum = runningSum.plus(j.resources());
                    Node n = nodes.get(j.nodeId());
                    if (n == null || n.status() != NodeStatus.UP) {
                        return false;
                    }
                }
            }
            return allocatedSum.equals(runningSum);
        }
    }
}
