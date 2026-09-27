package com.github.lechandonga.hpcgpufairschedulerjava.scheduler;

import com.github.lechandonga.hpcgpufairschedulerjava.scheduler.core.FairScheduler;
import com.github.lechandonga.hpcgpufairschedulerjava.scheduler.exception.SchedulingException;
import com.github.lechandonga.hpcgpufairschedulerjava.scheduler.exception.SchedulingException.Reason;
import com.github.lechandonga.hpcgpufairschedulerjava.scheduler.model.*;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 节点故障：下线 / 失联时运行中作业有明确处置（重排），不超卖；
 * 恢复后调度结果可复现；重复下线是幂等 no-op。
 */
class NodeFailureTest {

    private FairScheduler scheduler;

    @BeforeEach
    void setUp() {
        scheduler = new FairScheduler(
                List.of(new Node("node-1", new ResourceRequest(16, 4, 128_000)),
                        new Node("node-2", new ResourceRequest(16, 4, 128_000))),
                List.of(new QueueDef("a", 1), new QueueDef("b", 1)));
    }

    @AfterEach
    void printDecisions() {
        System.out.println("---- decision log ----");
        scheduler.getDecisionLog().forEach(System.out::println);
    }

    @Test
    void nodeDownRequeuesRunningJobsOntoSurvivingNodes() {
        // j1/j2 上 node-1（各 2 GPU），j3 上 node-2（2 GPU）
        scheduler.submit(new Job("j1", "a", 0, new ResourceRequest(1, 2, 100)));
        scheduler.submit(new Job("j2", "b", 0, new ResourceRequest(1, 2, 100)));
        scheduler.submit(new Job("j3", "a", 0, new ResourceRequest(1, 2, 100)));
        assertEquals("node-1", scheduler.getJob("j1").getAssignedNodeId());
        assertEquals("node-1", scheduler.getJob("j2").getAssignedNodeId());
        assertEquals("node-2", scheduler.getJob("j3").getAssignedNodeId());

        scheduler.nodeDown("node-1");

        // node-2 还有 2 GPU 空闲；j1(queue a)、j2(queue b) 同优先级竞争，
        // 队列 b 当前用量比(0) 小于队列 a(j3 在跑, 2/1)，故 j2 先被重排上 node-2
        assertEquals(JobStatus.RUNNING, scheduler.getJob("j2").getStatus());
        assertEquals("node-2", scheduler.getJob("j2").getAssignedNodeId());
        assertEquals(JobStatus.PENDING, scheduler.getJob("j1").getStatus());
        assertNull(scheduler.getJob("j1").getAssignedNodeId());
        // 故障节点账目清零，不允许残留占用
        assertEquals(ResourceRequest.ZERO, scheduler.getNodes().get("node-1").getAllocated());
        assertTrue(scheduler.accountingBalanced());
    }

    @Test
    void nodeDownIsIdempotent() {
        scheduler.submit(new Job("j1", "a", 0, new ResourceRequest(1, 2, 100)));
        scheduler.nodeDown("node-1");
        // j1 被重排到存活的 node-2 上
        assertEquals(JobStatus.RUNNING, scheduler.getJob("j1").getStatus());
        assertEquals("node-2", scheduler.getJob("j1").getAssignedNodeId());
        int logSize = scheduler.getDecisionLog().size();

        scheduler.nodeDown("node-1"); // 重复下线
        scheduler.nodeDown("node-1");

        // 状态不变：j1 仍在 node-2，故障节点账目为零
        assertEquals(JobStatus.RUNNING, scheduler.getJob("j1").getStatus());
        assertEquals("node-2", scheduler.getJob("j1").getAssignedNodeId());
        assertEquals(ResourceRequest.ZERO, scheduler.getNodes().get("node-1").getAllocated());
        assertTrue(scheduler.accountingBalanced());
        // 重复调用只追加幂等提示日志，不产生额外重排
        long requeues = scheduler.getDecisionLog().stream()
                .skip(logSize)
                .filter(r -> r.type() == DecisionRecord.Type.JOB_REQUEUED)
                .count();
        assertEquals(0, requeues);
    }

    @Test
    void nodeRecoveryReschedulesPendingJobsDeterministically() {
        Map<String, String> first = runFailureAndRecoveryScenario();
        Map<String, String> second = runFailureAndRecoveryScenario();
        System.out.println("placement after recovery: " + first);
        assertEquals(first, second, "recovery scheduling must be reproducible");
    }

    private Map<String, String> runFailureAndRecoveryScenario() {
        FairScheduler s = new FairScheduler(
                List.of(new Node("node-1", new ResourceRequest(16, 4, 128_000)),
                        new Node("node-2", new ResourceRequest(16, 4, 128_000))),
                List.of(new QueueDef("a", 1), new QueueDef("b", 1)));
        s.submit(new Job("j1", "a", 0, new ResourceRequest(1, 2, 100)));
        s.submit(new Job("j2", "b", 0, new ResourceRequest(1, 2, 100)));
        s.submit(new Job("j3", "a", 0, new ResourceRequest(1, 2, 100)));
        s.nodeDown("node-1");
        s.nodeRecover("node-1");
        return s.getJobs().stream()
                .filter(j -> j.getStatus() == JobStatus.RUNNING)
                .collect(Collectors.toMap(Job::getJobId, Job::getAssignedNodeId));
    }

    @Test
    void recoveredNodeTakesPendingJobsAgain() {
        scheduler.submit(new Job("j1", "a", 0, new ResourceRequest(1, 4, 100)));
        scheduler.submit(new Job("j2", "b", 0, new ResourceRequest(1, 4, 100)));
        scheduler.submit(new Job("j3", "a", 0, new ResourceRequest(1, 4, 100))); // 排队

        scheduler.nodeDown("node-1");
        // node-1 上的 j1 重排，但 node-2 已满 -> j1/j2? 实际 j1 排队
        assertEquals(JobStatus.PENDING, scheduler.getJob("j1").getStatus());

        scheduler.nodeRecover("node-1");
        // 恢复后按序重排：j1(seq 小) 先上 node-1
        assertEquals(JobStatus.RUNNING, scheduler.getJob("j1").getStatus());
        assertEquals("node-1", scheduler.getJob("j1").getAssignedNodeId());
        assertTrue(scheduler.accountingBalanced());
    }

    @Test
    void unknownNodeRejectedWithDistinctReason() {
        SchedulingException e = assertThrows(SchedulingException.class,
                () -> scheduler.nodeDown("no-such-node"));
        assertEquals(Reason.UNKNOWN_NODE, e.getReason());
    }

    @Test
    void noOversellAfterFailureAndRecovery() {
        // 故障-恢复循环中任何时刻节点分配都不超过容量
        for (int i = 0; i < 4; i++) {
            scheduler.submit(new Job("j" + i, i % 2 == 0 ? "a" : "b",
                    0, new ResourceRequest(1, 2, 100)));
        }
        for (int round = 0; round < 3; round++) {
            scheduler.nodeDown("node-1");
            assertNoOversell();
            scheduler.nodeRecover("node-1");
            assertNoOversell();
        }
        assertTrue(scheduler.accountingBalanced());
    }

    private void assertNoOversell() {
        for (Node node : scheduler.getNodes().values()) {
            assertTrue(node.getCapacity().canFit(node.getAllocated()),
                    "oversell on " + node.getNodeId());
        }
    }
}
