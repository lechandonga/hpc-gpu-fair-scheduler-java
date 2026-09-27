package com.github.lechandonga.hpcgpufairschedulerjava.scheduler.core;

import com.github.lechandonga.hpcgpufairschedulerjava.scheduler.exception.SchedulingException;
import com.github.lechandonga.hpcgpufairschedulerjava.scheduler.exception.SchedulingException.Reason;
import com.github.lechandonga.hpcgpufairschedulerjava.scheduler.model.*;
import com.github.lechandonga.hpcgpufairschedulerjava.scheduler.model.DecisionRecord.Type;

import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 本地集群公平调度器。
 *
 * <p>调度规则（全部确定性，不依赖遍历哈希顺序）：
 * <ul>
 *   <li>排队：优先级高者先调度；同优先级按队列权重公平（运行中 GPU/权重 比值最小者优先，
 *       比值相同按队列名字典序），队列内部按提交序号 FIFO。</li>
 *   <li>放置：全有或全无，按节点 id 字典序首次适配（first-fit）。</li>
 *   <li>抢占：高优先级作业放不下时，按 (优先级升序, 提交序号降序) 逐抢占低优先级运行作业，
 *       直到能放下为止；被抢占作业释放资源后重新排队，可再次被调度。</li>
 *   <li>节点故障：节点 DOWN 后其上作业全部重排；恢复后容量重新纳入调度。</li>
 * </ul>
 * 所有公共方法 synchronized，保证并发提交下结论一致。
 */
public class FairScheduler {

    private final Map<String, Node> nodes;      // key 有序（构造时按 id 排序）
    private final Map<String, QueueDef> queues;
    private final Map<String, Job> jobs = new LinkedHashMap<>();
    private final List<DecisionRecord> decisionLog = new ArrayList<>();
    private final AtomicLong seq = new AtomicLong();

    public FairScheduler(List<Node> nodes, List<QueueDef> queues) {
        this.nodes = new TreeMap<>();
        for (Node n : nodes) {
            this.nodes.put(n.getNodeId(), n);
        }
        this.queues = new LinkedHashMap<>();
        for (QueueDef q : queues) {
            this.queues.put(q.name(), q);
        }
    }

    // ---------------- 提交 ----------------

    public synchronized void submit(Job job) {
        validate(job);
        job.setSubmitSeq(seq.getAndIncrement());
        job.setStatus(JobStatus.PENDING);
        jobs.put(job.getJobId(), job);
        log(Type.SUBMIT_ACCEPTED, job.getJobId() + " accepted, seq=" + job.getSubmitSeq()
                + ", queue=" + job.getQueueName() + ", prio=" + job.getPriority()
                + ", req=" + job.getRequest());
        scheduleRound();
    }

    private void validate(Job job) {
        if (job.getJobId() == null || job.getJobId().isBlank()) {
            throw new SchedulingException(Reason.INVALID_REQUEST, "jobId must not be blank");
        }
        if (job.getRequest() == null || job.getRequest().isZero()) {
            throw new SchedulingException(Reason.INVALID_REQUEST,
                    "resource request must be positive: " + job.getRequest());
        }
        if (!queues.containsKey(job.getQueueName())) {
            throw new SchedulingException(Reason.UNKNOWN_QUEUE,
                    "unknown queue: " + job.getQueueName());
        }
        if (jobs.containsKey(job.getJobId())) {
            throw new SchedulingException(Reason.DUPLICATE_JOB,
                    "duplicate jobId: " + job.getJobId());
        }
        // 永远无法满足（超过最大单节点容量）才拒绝；暂时资源紧张的进入排队
        boolean everFittable = nodes.values().stream()
                .anyMatch(n -> n.getCapacity().canFit(job.getRequest()));
        if (!everFittable) {
            throw new SchedulingException(Reason.INSUFFICIENT_RESOURCE,
                    "request " + job.getRequest() + " exceeds capacity of every node");
        }
    }

    // ---------------- 作业完成 ----------------

    public synchronized void complete(String jobId) {
        Job job = mustGetJob(jobId);
        if (job.getStatus() != JobStatus.RUNNING) {
            throw new SchedulingException(Reason.ILLEGAL_STATE,
                    "cannot complete job " + jobId + " in status " + job.getStatus());
        }
        releaseFromNode(job);
        job.setStatus(JobStatus.COMPLETED);
        job.setAssignedNodeId(null);
        log(Type.JOB_COMPLETED, jobId + " completed, resources released: " + job.getRequest());
        scheduleRound();
    }

    // ---------------- 节点上下线 ----------------

    public synchronized void nodeDown(String nodeId) {
        Node node = mustGetNode(nodeId);
        if (node.getStatus() != NodeStatus.UP) {
            log(Type.NODE_DOWN, nodeId + " already " + node.getStatus() + ", idempotent no-op");
            return;
        }
        node.setStatus(NodeStatus.DOWN);
        log(Type.NODE_DOWN, nodeId + " went DOWN, requeueing " + node.getRunningJobIds().size()
                + " running jobs");
        // 按提交序号排序，保证重排顺序确定
        List<Job> affected = node.getRunningJobIds().stream()
                .map(jobs::get)
                .sorted(Comparator.comparingLong(Job::getSubmitSeq))
                .toList();
        for (Job job : affected) {
            releaseFromNode(job);
            job.setStatus(JobStatus.PENDING);
            job.setAssignedNodeId(null);
            log(Type.JOB_REQUEUED, job.getJobId() + " requeued due to node " + nodeId + " DOWN");
        }
        scheduleRound();
    }

    public synchronized void nodeRecover(String nodeId) {
        Node node = mustGetNode(nodeId);
        if (node.getStatus() == NodeStatus.UP) {
            log(Type.NODE_RECOVERED, nodeId + " already UP, idempotent no-op");
            return;
        }
        node.setStatus(NodeStatus.UP);
        log(Type.NODE_RECOVERED, nodeId + " recovered, capacity " + node.getCapacity()
                + " back in scheduling");
        scheduleRound();
    }

    // ---------------- 查询 ----------------

    public synchronized Job getJob(String jobId) {
        return mustGetJob(jobId);
    }

    public synchronized List<Job> getJobs() {
        return List.copyOf(jobs.values());
    }

    public synchronized Map<String, Node> getNodes() {
        return Collections.unmodifiableMap(nodes);
    }

    public synchronized List<DecisionRecord> getDecisionLog() {
        return List.copyOf(decisionLog);
    }

    /** 账目核对：所有节点已分配资源之和 == 所有 RUNNING 作业请求之和。 */
    public synchronized boolean accountingBalanced() {
        ResourceRequest nodeSide = ResourceRequest.ZERO;
        for (Node n : nodes.values()) {
            nodeSide = nodeSide.plus(n.getAllocated());
        }
        ResourceRequest jobSide = ResourceRequest.ZERO;
        for (Job j : jobs.values()) {
            if (j.getStatus() == JobStatus.RUNNING) {
                jobSide = jobSide.plus(j.getRequest());
            }
        }
        return nodeSide.equals(jobSide);
    }

    // ---------------- 调度主循环 ----------------

    /**
     * 一轮调度：反复取出队首作业尝试放置；放不下则尝试抢占；
     * 仍放不下则本轮搁置（deferred），直到一整轮无任何进展。
     */
    private void scheduleRound() {
        Set<String> deferred = new HashSet<>();
        while (true) {
            Job next = pickNext(deferred);
            if (next == null) {
                return;
            }
            if (tryPlace(next)) {
                continue;
            }
            if (!tryPreemptAndPlace(next)) {
                deferred.add(next.getJobId());
            }
        }
    }

    /** 选出下一个待调度作业：最高优先级 -> 队列权重比最小 -> 队列内 FIFO。 */
    private Job pickNext(Set<String> deferred) {
        List<Job> pending = jobs.values().stream()
                .filter(j -> j.getStatus() == JobStatus.PENDING)
                .filter(j -> !deferred.contains(j.getJobId()))
                .toList();
        if (pending.isEmpty()) {
            return null;
        }
        int maxPrio = pending.stream().mapToInt(Job::getPriority).max().orElseThrow();
        List<Job> top = pending.stream().filter(j -> j.getPriority() == maxPrio).toList();

        // 在有待调度作业的队列中，选 运行GPU/权重 最小的队列（饿死防护：用量为 0 比值即 0）
        String chosenQueue = top.stream()
                .map(Job::getQueueName)
                .distinct()
                .min(Comparator
                        .comparingDouble(this::queueUsageRatio)
                        .thenComparing(Comparator.naturalOrder()))
                .orElseThrow();
        Job chosen = top.stream()
                .filter(j -> j.getQueueName().equals(chosenQueue))
                .min(Comparator.comparingLong(Job::getSubmitSeq))
                .orElseThrow();
        log(Type.QUEUE_SELECTED, "queue=" + chosenQueue + " (usageRatio="
                + queueUsageRatio(chosenQueue) + ", weight="
                + queues.get(chosenQueue).weight() + "), picked " + chosen.getJobId()
                + " prio=" + maxPrio);
        return chosen;
    }

    /** 队列公平性度量：运行中 GPU 数 / 权重。 */
    private double queueUsageRatio(String queueName) {
        long runningGpu = jobs.values().stream()
                .filter(j -> j.getStatus() == JobStatus.RUNNING)
                .filter(j -> j.getQueueName().equals(queueName))
                .mapToLong(j -> j.getRequest().gpus())
                .sum();
        return (double) runningGpu / queues.get(queueName).weight();
    }

    /** 全有或全无放置：按节点 id 顺序 first-fit。 */
    private boolean tryPlace(Job job) {
        for (Node node : nodes.values()) {
            if (node.canFit(job.getRequest())) {
                node.allocate(job.getJobId(), job.getRequest());
                job.setStatus(JobStatus.RUNNING);
                job.setAssignedNodeId(node.getNodeId());
                log(Type.JOB_SCHEDULED, job.getJobId() + " placed on " + node.getNodeId()
                        + " req=" + job.getRequest() + " (all-or-nothing first-fit)");
                return true;
            }
        }
        return false;
    }

    /**
     * 为 job 腾出空间：按 (优先级升序, 提交序号降序) 抢占低优先级运行作业。
     * 每抢占一个就重试放置；被抢占作业立即重排，本轮之后可被重新调度。
     */
    private boolean tryPreemptAndPlace(Job job) {
        List<Job> victims = jobs.values().stream()
                .filter(j -> j.getStatus() == JobStatus.RUNNING)
                .filter(j -> j.getPriority() < job.getPriority())
                .sorted(Comparator.comparingInt(Job::getPriority)
                        .thenComparing(Comparator.comparingLong(Job::getSubmitSeq).reversed()))
                .toList();
        if (victims.isEmpty()) {
            log(Type.PREEMPTION_SKIPPED, job.getJobId() + " cannot be placed and no lower-priority"
                    + " running job to preempt");
            return false;
        }
        for (Job victim : victims) {
            // 幂等保障：只有仍处于 RUNNING 的作业才会被抢占，重复触发不会重复扣减
            if (victim.getStatus() != JobStatus.RUNNING) {
                continue;
            }
            preempt(victim);
            if (tryPlace(job)) {
                return true;
            }
        }
        log(Type.PREEMPTION_SKIPPED, job.getJobId() + " still cannot be placed after preempting "
                + victims.size() + " victim(s)");
        return false;
    }

    private void preempt(Job victim) {
        releaseFromNode(victim);
        victim.setStatus(JobStatus.PREEMPTED);
        victim.incrementPreemptionCount();
        victim.setAssignedNodeId(null);
        log(Type.JOB_PREEMPTED, victim.getJobId() + " preempted (count="
                + victim.getPreemptionCount() + "), released " + victim.getRequest());
        victim.setStatus(JobStatus.PENDING);
        log(Type.JOB_REQUEUED, victim.getJobId() + " back to PENDING after preemption");
    }

    // ---------------- 工具 ----------------

    private void releaseFromNode(Job job) {
        Node node = nodes.get(job.getAssignedNodeId());
        if (node == null) {
            throw new IllegalStateException("job " + job.getJobId() + " has no assigned node");
        }
        node.release(job.getJobId(), job.getRequest());
    }

    private Job mustGetJob(String jobId) {
        Job job = jobs.get(jobId);
        if (job == null) {
            throw new SchedulingException(Reason.UNKNOWN_JOB, "unknown job: " + jobId);
        }
        return job;
    }

    private Node mustGetNode(String nodeId) {
        Node node = nodes.get(nodeId);
        if (node == null) {
            throw new SchedulingException(Reason.UNKNOWN_NODE, "unknown node: " + nodeId);
        }
        return node;
    }

    private void log(Type type, String detail) {
        decisionLog.add(new DecisionRecord(type, detail));
    }
}
