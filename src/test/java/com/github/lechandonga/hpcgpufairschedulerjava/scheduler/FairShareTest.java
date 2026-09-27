package com.github.lechandonga.hpcgpufairschedulerjava.scheduler;

import com.github.lechandonga.hpcgpufairschedulerjava.scheduler.core.FairScheduler;
import com.github.lechandonga.hpcgpufairschedulerjava.scheduler.model.*;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 公平分配：同优先级下，运行中的共享资源按队列权重收敛；
 * 权重小的队列也会持续得到调度，不会被饿死。
 */
class FairShareTest {

    private FairScheduler scheduler;

    @AfterEach
    void printDecisions() {
        if (scheduler == null) {
            return;
        }
        System.out.println("---- decision log (QUEUE_SELECTED / JOB_SCHEDULED) ----");
        scheduler.getDecisionLog().stream()
                .filter(r -> r.type() == DecisionRecord.Type.QUEUE_SELECTED
                        || r.type() == DecisionRecord.Type.JOB_SCHEDULED)
                .forEach(System.out::println);
    }

    @Test
    void runningGpuShareConvergesToQueueWeights() {
        // 1 节点 8 GPU；队列 heavy:light = 3:1。
        // 先用 heavy 作业占满集群，再让 heavy/light 一起排队，
        // 随着运行作业完成，空出的槽位应按权重比补给排队队列。
        scheduler = new FairScheduler(
                List.of(new Node("node-1", new ResourceRequest(64, 8, 256_000))),
                List.of(new QueueDef("heavy", 3), new QueueDef("light", 1)));

        for (int i = 0; i < 8; i++) {
            scheduler.submit(new Job("fill" + i, "heavy", 0, new ResourceRequest(1, 1, 100)));
        }
        for (int i = 0; i < 8; i++) {
            scheduler.submit(new Job("h" + i, "heavy", 0, new ResourceRequest(1, 1, 100)));
            scheduler.submit(new Job("l" + i, "light", 0, new ResourceRequest(1, 1, 100)));
        }
        // 每次完成一个，空出的槽位立即按权重公平规则补给排队作业
        for (int i = 0; i < 8; i++) {
            scheduler.complete("fill" + i);
        }

        Map<String, Long> runningGpuByQueue = scheduler.getJobs().stream()
                .filter(j -> j.getStatus() == JobStatus.RUNNING)
                .collect(Collectors.groupingBy(Job::getQueueName, Collectors.counting()));

        System.out.println("running by queue: " + runningGpuByQueue);
        // 8 GPU 按 3:1 收敛 -> heavy 6, light 2
        assertEquals(6L, runningGpuByQueue.get("heavy"));
        assertEquals(2L, runningGpuByQueue.get("light"));
        assertTrue(scheduler.accountingBalanced());
    }

    @Test
    void lowWeightQueueIsNotStarved() {
        // 权重 1 的小队列面对权重 10 的大队列，也必须拿到份额
        scheduler = new FairScheduler(
                List.of(new Node("node-1", new ResourceRequest(64, 11, 256_000))),
                List.of(new QueueDef("big", 10), new QueueDef("small", 1)));

        for (int i = 0; i < 11; i++) {
            scheduler.submit(new Job("fill" + i, "big", 0, new ResourceRequest(1, 1, 100)));
        }
        for (int i = 0; i < 11; i++) {
            scheduler.submit(new Job("big" + i, "big", 0, new ResourceRequest(1, 1, 100)));
            scheduler.submit(new Job("small" + i, "small", 0, new ResourceRequest(1, 1, 100)));
        }
        for (int i = 0; i < 11; i++) {
            scheduler.complete("fill" + i);
        }

        Map<String, Long> runningByQueue = scheduler.getJobs().stream()
                .filter(j -> j.getStatus() == JobStatus.RUNNING)
                .collect(Collectors.groupingBy(Job::getQueueName, Collectors.counting()));
        System.out.println("running by queue: " + runningByQueue);
        // 11 GPU 按 10:1 收敛 -> big 10, small 1（小队列不被饿死）
        assertEquals(10L, runningByQueue.get("big"));
        assertEquals(1L, runningByQueue.get("small"));
    }

    @Test
    void schedulingIsDeterministicAcrossRepeats() {
        // 同样输入跑两次，放置结论必须完全一致
        Map<String, String> first = runScenario();
        Map<String, String> second = runScenario();
        assertEquals(first, second, "same inputs must reproduce same placement");
    }

    private Map<String, String> runScenario() {
        FairScheduler s = new FairScheduler(
                List.of(new Node("node-1", new ResourceRequest(16, 4, 128_000)),
                        new Node("node-2", new ResourceRequest(16, 4, 128_000))),
                List.of(new QueueDef("a", 2), new QueueDef("b", 1)));
        for (int i = 0; i < 6; i++) {
            s.submit(new Job("a" + i, "a", 0, new ResourceRequest(1, 1, 100)));
            s.submit(new Job("b" + i, "b", 0, new ResourceRequest(1, 1, 100)));
        }
        return s.getJobs().stream()
                .filter(j -> j.getStatus() == JobStatus.RUNNING)
                .collect(Collectors.toMap(Job::getJobId, Job::getAssignedNodeId));
    }
}
