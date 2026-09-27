package com.github.lechandonga.hpcgpufairschedulerjava.scheduler;

import com.github.lechandonga.hpcgpufairschedulerjava.scheduler.core.Scheduler;
import com.github.lechandonga.hpcgpufairschedulerjava.scheduler.model.Job;
import com.github.lechandonga.hpcgpufairschedulerjava.scheduler.model.JobState;
import com.github.lechandonga.hpcgpufairschedulerjava.scheduler.model.Node;
import com.github.lechandonga.hpcgpufairschedulerjava.scheduler.model.ResourceSpec;
import org.junit.jupiter.api.Test;

import static com.github.lechandonga.hpcgpufairschedulerjava.scheduler.SchedulerTestSupport.req;
import static com.github.lechandonga.hpcgpufairschedulerjava.scheduler.SchedulerTestSupport.standardCluster;
import static org.junit.jupiter.api.Assertions.*;

/** 全有或全无放置：资源不得被拆分、部分放置失败不得留下半截账目。 */
class AllOrNothingPlacementTest {

    @Test
    void jobIsNeverSplitAcrossNodes() {
        Scheduler s = standardCluster(); // 两节点各 4 GPU
        // 两队列各占 3 GPU（alpha 未超份额、beta 恰好满份额，均不触发抢占），
        // 剩余 1+1=2 GPU 分散在两节点
        s.submit(req("a1", "alpha", 1, 3));
        s.submit(req("a2", "beta", 1, 3));
        // 需要 2 GPU 的作业：集群总剩余够（2），但任何单节点都不够 -> 必须排队，不得拆分
        s.submit(req("big", "beta", 1, 2));
        Job big = s.job("big").orElseThrow();
        assertEquals(JobState.PENDING, big.state(), "单节点放不下时必须整体排队，不得跨节点拆分");
        assertNull(big.nodeId());
        // 账目：只有两个 3-GPU 作业在运行
        assertTrue(s.accountingConsistent());
        assertEquals(2, s.jobsInState(JobState.RUNNING).size());
        s.decisionLog().printAll();
    }

    @Test
    void fragmentedSpaceIsReclaimedAfterCompletion() {
        Scheduler s = standardCluster();
        s.submit(req("a1", "alpha", 1, 3));
        s.submit(req("a2", "beta", 1, 3));
        s.submit(req("big", "beta", 1, 2));
        assertEquals(JobState.PENDING, s.job("big").orElseThrow().state());

        // 完成一个 3-GPU 作业后，big 应能整体放置
        assertTrue(s.complete("a1"));
        Job big = s.job("big").orElseThrow();
        assertEquals(JobState.RUNNING, big.state());
        assertNotNull(big.nodeId());
        assertTrue(s.accountingConsistent());
        s.decisionLog().printAll();
    }

    @Test
    void multiDimensionalAllOrNothing() {
        Scheduler s = new Scheduler();
        s.addQueue("alpha", 1.0);
        s.addNode(new Node("n1", new ResourceSpec(4, 8, 32_000)));
        // GPU 够但内存不够的作业：整体不得放置
        s.submit(new com.github.lechandonga.hpcgpufairschedulerjava.scheduler.model.JobRequest(
                "mem-hog", "alpha", 1, new ResourceSpec(1, 2, 40_000)));
        // 该请求单节点内存不足 -> INVALID_REQUEST 拒绝（永远无法放置）
        assertTrue(s.job("mem-hog").isEmpty(), "无法满足的全有或全无请求不得进入队列");

        // 合法大请求占满内存后，后续小作业因内存不足排队而不是部分放置
        s.submit(new com.github.lechandonga.hpcgpufairschedulerjava.scheduler.model.JobRequest(
                "hog", "alpha", 1, new ResourceSpec(1, 2, 32_000)));
        s.submit(new com.github.lechandonga.hpcgpufairschedulerjava.scheduler.model.JobRequest(
                "small", "alpha", 1, new ResourceSpec(1, 1, 1_000)));
        assertEquals(JobState.RUNNING, s.job("hog").orElseThrow().state());
        assertEquals(JobState.PENDING, s.job("small").orElseThrow().state(),
                "内存维度不足时整体排队，GPU 空闲也不得部分放置");
        assertTrue(s.accountingConsistent());
        s.decisionLog().printAll();
    }
}
