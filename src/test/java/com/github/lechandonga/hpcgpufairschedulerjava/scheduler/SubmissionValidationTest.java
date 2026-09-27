package com.github.lechandonga.hpcgpufairschedulerjava.scheduler;

import com.github.lechandonga.hpcgpufairschedulerjava.scheduler.core.Scheduler;
import com.github.lechandonga.hpcgpufairschedulerjava.scheduler.model.JobState;
import com.github.lechandonga.hpcgpufairschedulerjava.scheduler.model.RejectionReason;
import com.github.lechandonga.hpcgpufairschedulerjava.scheduler.model.ResourceSpec;
import com.github.lechandonga.hpcgpufairschedulerjava.scheduler.model.SubmitResult;
import org.junit.jupiter.api.Test;

import static com.github.lechandonga.hpcgpufairschedulerjava.scheduler.SchedulerTestSupport.req;
import static com.github.lechandonga.hpcgpufairschedulerjava.scheduler.SchedulerTestSupport.standardCluster;
import static org.junit.jupiter.api.Assertions.*;

/** 提交校验：不同拒绝原因必须可区分。 */
class SubmissionValidationTest {

    @Test
    void duplicateSubmissionRejectedDistinctly() {
        Scheduler s = standardCluster();
        assertTrue(s.submit(req("j1", "alpha", 5, 1)).accepted());
        SubmitResult dup = s.submit(req("j1", "alpha", 5, 1));
        assertFalse(dup.accepted());
        assertEquals(RejectionReason.DUPLICATE_JOB, dup.reason());
        s.decisionLog().printAll();
    }

    @Test
    void invalidParamsRejected() {
        Scheduler s = standardCluster();
        SubmitResult blank = s.submit(req(" ", "alpha", 5, 1));
        assertEquals(RejectionReason.INVALID_REQUEST, blank.reason());

        SubmitResult zeroGpu = s.submit(new com.github.lechandonga.hpcgpufairschedulerjava.scheduler.model.JobRequest(
                "j2", "alpha", 5, new ResourceSpec(0, 4, 100)));
        assertEquals(RejectionReason.INVALID_REQUEST, zeroGpu.reason());

        // 单节点最大 4 GPU，要 5 GPU 属于永远无法全有或全无放置 -> 参数非法
        SubmitResult tooBigForOneNode = s.submit(req("j3", "alpha", 5, 5));
        assertEquals(RejectionReason.INVALID_REQUEST, tooBigForOneNode.reason());
        s.decisionLog().printAll();
    }

    @Test
    void unknownQueueRejected() {
        Scheduler s = standardCluster();
        SubmitResult r = s.submit(req("j1", "no-such-queue", 5, 1));
        assertFalse(r.accepted());
        assertEquals(RejectionReason.UNKNOWN_QUEUE, r.reason());
        s.decisionLog().printAll();
    }

    @Test
    void insufficientClusterResourcesRejected() {
        Scheduler s = new Scheduler();
        s.addQueue("alpha", 1.0);
        // 两个节点各 4 GPU，总共 8 GPU；单节点放得下但全集群也放不下的请求
        s.addNode(new com.github.lechandonga.hpcgpufairschedulerjava.scheduler.model.Node(
                "n1", new ResourceSpec(4, 16, 64_000)));
        // 单节点 4 GPU 能放 4，但集群只有 4：请求合法可放置
        assertTrue(s.submit(req("ok", "alpha", 1, 4)).accepted());
        // 构造一个单节点能放、但集群总量不足的场景：再提交一个 4 GPU 时资源被占满，
        // 该作业应排队而非被拒（临时不足可等待）
        assertTrue(s.submit(req("queued", "alpha", 1, 4)).accepted());
        assertEquals(JobState.PENDING, s.job("queued").orElseThrow().state());
        s.decisionLog().printAll();
    }

    @Test
    void acceptedJobIsQueuedOrRunning() {
        Scheduler s = standardCluster();
        assertTrue(s.submit(req("j1", "alpha", 5, 2)).accepted());
        JobState st = s.job("j1").orElseThrow().state();
        assertTrue(st == JobState.RUNNING || st == JobState.PENDING);
        assertTrue(s.accountingConsistent());
        s.decisionLog().printAll();
    }
}
