package com.github.lechandonga.hpcgpufairschedulerjava.scheduler;

import com.github.lechandonga.hpcgpufairschedulerjava.scheduler.core.FairScheduler;
import com.github.lechandonga.hpcgpufairschedulerjava.scheduler.model.*;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 抢占：高优先级作业抢占低优先级运行作业；
 * 抢占后被抢占者、排队者、运行者的资源账目守恒；
 * 重复触发同一轮调度不会重复扣减。
 */
class PreemptionTest {

    private FairScheduler scheduler;

    @BeforeEach
    void setUp() {
        // 2 节点 x 4 GPU，同权重两条队列
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

    private void fillClusterWithLowPriorityJobs() {
        // low1/low2 -> node-1, low3/low4 -> node-2，各占 2 GPU，集群占满
        for (int i = 1; i <= 4; i++) {
            scheduler.submit(new Job("low" + i, i % 2 == 0 ? "b" : "a",
                    0, new ResourceRequest(1, 2, 100)));
        }
        assertEquals(8, scheduler.getNodes().get("node-1").getAllocated().gpus()
                + scheduler.getNodes().get("node-2").getAllocated().gpus());
    }

    @Test
    void highPriorityJobPreemptsLowestPriorityNewestVictim() {
        fillClusterWithLowPriorityJobs();

        scheduler.submit(new Job("high", "a", 10, new ResourceRequest(1, 2, 100)));

        assertEquals(JobStatus.RUNNING, scheduler.getJob("high").getStatus());
        // 受害者选择规则：优先级最低、提交序号最大 -> low4
        Job victim = scheduler.getJob("low4");
        assertEquals(JobStatus.PENDING, victim.getStatus());
        assertEquals(1, victim.getPreemptionCount());
        assertNull(victim.getAssignedNodeId());
        // 其余低优先级作业不受影响
        for (String id : List.of("low1", "low2", "low3")) {
            assertEquals(JobStatus.RUNNING, scheduler.getJob(id).getStatus(), id);
        }
        // 账目守恒：运行中 4 个作业 x 2 GPU = 8 GPU = 两节点已分配之和
        assertTrue(scheduler.accountingBalanced());
        assertEquals(8, scheduler.getNodes().values().stream()
                .mapToInt(n -> n.getAllocated().gpus()).sum());
    }

    @Test
    void preemptedJobIsRescheduledWhenResourcesFreeUp() {
        fillClusterWithLowPriorityJobs();
        scheduler.submit(new Job("high", "a", 10, new ResourceRequest(1, 2, 100)));
        assertEquals(JobStatus.PENDING, scheduler.getJob("low4").getStatus());

        scheduler.complete("high");

        Job low4 = scheduler.getJob("low4");
        assertEquals(JobStatus.RUNNING, low4.getStatus());
        assertNotNull(low4.getAssignedNodeId());
        assertTrue(scheduler.accountingBalanced());
    }

    @Test
    void repeatedSchedulingRoundsDoNotDoubleDeduct() {
        fillClusterWithLowPriorityJobs();
        scheduler.submit(new Job("high", "a", 10, new ResourceRequest(1, 2, 100)));

        int gpusAfterPreemption = scheduler.getNodes().values().stream()
                .mapToInt(n -> n.getAllocated().gpus()).sum();
        int preemptions = scheduler.getJob("low4").getPreemptionCount();

        // 再触发多轮调度（提交一个放不下的作业会反复走 scheduleRound）
        scheduler.submit(new Job("extra", "b", 0, new ResourceRequest(1, 4, 100)));
        scheduler.submit(new Job("extra2", "b", 0, new ResourceRequest(1, 4, 100)));

        assertEquals(gpusAfterPreemption, scheduler.getNodes().values().stream()
                .mapToInt(n -> n.getAllocated().gpus()).sum(),
                "repeated rounds must not change allocation");
        assertEquals(preemptions, scheduler.getJob("low4").getPreemptionCount(),
                "same preemption must not be applied twice");
        assertTrue(scheduler.accountingBalanced());
    }

    @Test
    void preemptionStopsAsSoonAsJobFits() {
        fillClusterWithLowPriorityJobs();
        // high 只需要 2 GPU，抢占 low4 一个受害者即够，low3 不应被波及
        scheduler.submit(new Job("high", "a", 10, new ResourceRequest(1, 2, 100)));
        assertEquals(0, scheduler.getJob("low3").getPreemptionCount());
        assertEquals(JobStatus.RUNNING, scheduler.getJob("low3").getStatus());
    }

    @Test
    void noPreemptionAmongEqualPriority() {
        fillClusterWithLowPriorityJobs();
        // 同优先级作业放不下时只能排队，不触发抢占
        scheduler.submit(new Job("x", "a", 0, new ResourceRequest(1, 2, 100)));
        assertEquals(JobStatus.PENDING, scheduler.getJob("x").getStatus());
        for (String id : List.of("low1", "low2", "low3", "low4")) {
            assertEquals(JobStatus.RUNNING, scheduler.getJob(id).getStatus(), id);
            assertEquals(0, scheduler.getJob(id).getPreemptionCount());
        }
        assertTrue(scheduler.accountingBalanced());
    }
}
