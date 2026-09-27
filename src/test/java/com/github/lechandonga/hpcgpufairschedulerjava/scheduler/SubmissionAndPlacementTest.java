package com.github.lechandonga.hpcgpufairschedulerjava.scheduler;

import com.github.lechandonga.hpcgpufairschedulerjava.scheduler.core.FairScheduler;
import com.github.lechandonga.hpcgpufairschedulerjava.scheduler.exception.SchedulingException;
import com.github.lechandonga.hpcgpufairschedulerjava.scheduler.exception.SchedulingException.Reason;
import com.github.lechandonga.hpcgpufairschedulerjava.scheduler.model.*;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 提交校验：非法参数、重复提交、资源永远不足必须被区分开地拒绝；
 * 暂时放不下的作业进入排队而不是被拒绝；放置是全有或全无。
 */
class SubmissionAndPlacementTest {

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
    void rejectsInvalidRequestWithDistinctReason() {
        SchedulingException e = assertThrows(SchedulingException.class, () ->
                scheduler.submit(new Job("j0", "a", 0, ResourceRequest.ZERO)));
        assertEquals(Reason.INVALID_REQUEST, e.getReason());
    }

    @Test
    void rejectsUnknownQueueWithDistinctReason() {
        SchedulingException e = assertThrows(SchedulingException.class, () ->
                scheduler.submit(new Job("j0", "no-such-queue", 0,
                        new ResourceRequest(1, 1, 100))));
        assertEquals(Reason.UNKNOWN_QUEUE, e.getReason());
    }

    @Test
    void rejectsDuplicateJobIdWithDistinctReason() {
        scheduler.submit(new Job("dup", "a", 0, new ResourceRequest(1, 1, 100)));
        SchedulingException e = assertThrows(SchedulingException.class, () ->
                scheduler.submit(new Job("dup", "a", 0, new ResourceRequest(1, 1, 100))));
        assertEquals(Reason.DUPLICATE_JOB, e.getReason());
    }

    @Test
    void rejectsRequestExceedingEveryNode() {
        SchedulingException e = assertThrows(SchedulingException.class, () ->
                scheduler.submit(new Job("too-big", "a", 0,
                        new ResourceRequest(1, 5, 100)))); // 单节点最多 4 GPU
        assertEquals(Reason.INSUFFICIENT_RESOURCE, e.getReason());
    }

    @Test
    void queuesJobWhenResourcesTemporarilyBusy() {
        // 占满 node-1 和 node-2 的 GPU
        scheduler.submit(new Job("f1", "a", 0, new ResourceRequest(1, 4, 100)));
        scheduler.submit(new Job("f2", "b", 0, new ResourceRequest(1, 4, 100)));
        assertEquals(JobStatus.RUNNING, scheduler.getJob("f1").getStatus());
        assertEquals(JobStatus.RUNNING, scheduler.getJob("f2").getStatus());

        // 需要 4 GPU 的作业暂时放不下 -> 排队而非拒绝
        scheduler.submit(new Job("waiting", "a", 0, new ResourceRequest(1, 4, 100)));
        assertEquals(JobStatus.PENDING, scheduler.getJob("waiting").getStatus());
    }

    @Test
    void placementIsAllOrNothing() {
        // node-1 剩 1 GPU，node-2 剩 1 GPU，作业要 2 GPU：任何单节点都放不下 -> 排队
        scheduler.submit(new Job("p1", "a", 0, new ResourceRequest(1, 3, 100)));
        scheduler.submit(new Job("p2", "b", 0, new ResourceRequest(1, 3, 100)));
        scheduler.submit(new Job("gang", "a", 0, new ResourceRequest(1, 2, 100)));

        Job gang = scheduler.getJob("gang");
        assertEquals(JobStatus.PENDING, gang.getStatus(),
                "2-GPU job must not be split across nodes");
        // 不能出现部分扣减：两个节点各只被 p1/p2 占了 3 GPU
        assertEquals(3, scheduler.getNodes().get("node-1").getAllocated().gpus());
        assertEquals(3, scheduler.getNodes().get("node-2").getAllocated().gpus());
        assertTrue(scheduler.accountingBalanced());
    }

    @Test
    void pendingJobScheduledAfterCompletionFreesResources() {
        scheduler.submit(new Job("f1", "a", 0, new ResourceRequest(1, 4, 100)));
        scheduler.submit(new Job("f2", "b", 0, new ResourceRequest(1, 4, 100)));
        scheduler.submit(new Job("waiting", "a", 0, new ResourceRequest(1, 4, 100)));

        scheduler.complete("f1");
        assertEquals(JobStatus.RUNNING, scheduler.getJob("waiting").getStatus());
        assertEquals("node-1", scheduler.getJob("waiting").getAssignedNodeId());
        assertTrue(scheduler.accountingBalanced());
    }
}
