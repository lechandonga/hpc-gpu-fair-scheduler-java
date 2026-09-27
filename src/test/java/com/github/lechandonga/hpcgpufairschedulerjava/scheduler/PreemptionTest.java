package com.github.lechandonga.hpcgpufairschedulerjava.scheduler;

import com.github.lechandonga.hpcgpufairschedulerjava.scheduler.core.Scheduler;
import com.github.lechandonga.hpcgpufairschedulerjava.scheduler.model.Job;
import com.github.lechandonga.hpcgpufairschedulerjava.scheduler.model.JobState;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static com.github.lechandonga.hpcgpufairschedulerjava.scheduler.SchedulerTestSupport.req;
import static com.github.lechandonga.hpcgpufairschedulerjava.scheduler.SchedulerTestSupport.standardCluster;
import static org.junit.jupiter.api.Assertions.*;

/** 抢占：公平份额收敛、优先级抢占、重复触发幂等、账目守恒。 */
class PreemptionTest {

    @Test
    void starvingQueuePreemptsOverShareQueue() {
        Scheduler s = standardCluster(); // 8 GPU，alpha:beta = 2:1 -> 份额 5:3
        // beta 独占全部 8 GPU（超出其公平份额 3）
        for (int i = 0; i < 8; i++) {
            assertTrue(s.submit(req("beta-" + i, "beta", 1, 1)).accepted());
        }
        assertEquals(8, s.usageByQueue().get("beta"));

        // 欠账队列 alpha 提交 2 GPU 作业 -> 应抢占 beta 的超额部分
        assertTrue(s.submit(req("alpha-big", "alpha", 1, 2)).accepted());
        Job alphaBig = s.job("alpha-big").orElseThrow();
        assertEquals(JobState.RUNNING, alphaBig.state(), "欠账队列作业应通过抢占获得资源");

        List<Job> preempted = s.jobsInState(JobState.PREEMPTED);
        assertEquals(2, preempted.size(), "恰好抢占 2 个 1-GPU 作业，不多不少");
        assertTrue(preempted.stream().allMatch(j -> j.queue().equals("beta")));
        assertEquals(6, s.usageByQueue().get("beta"));
        assertEquals(2, s.usageByQueue().get("alpha"));
        assertTrue(s.accountingConsistent(), "抢占后账目必须守恒");
        s.decisionLog().printAll();
    }

    @Test
    void higherPriorityJobPreemptsLowerPriority() {
        Scheduler s = standardCluster();
        // 低优先级作业占满集群
        for (int i = 0; i < 4; i++) {
            s.submit(req("low-" + i, "alpha", 1, 2));
        }
        assertEquals(8, s.usageByQueue().get("alpha"));

        // 同队列高优先级作业到来 -> 抢占低优先级
        s.submit(req("vip", "alpha", 9, 2));
        assertEquals(JobState.RUNNING, s.job("vip").orElseThrow().state());
        assertEquals(1, s.jobsInState(JobState.PREEMPTED).size());
        assertTrue(s.accountingConsistent());
        s.decisionLog().printAll();
    }

    @Test
    void duplicatePreemptionTriggerIsIdempotent() {
        Scheduler s = standardCluster();
        for (int i = 0; i < 8; i++) {
            s.submit(req("beta-" + i, "beta", 1, 1));
        }
        s.submit(req("alpha-big", "alpha", 1, 2));
        assertEquals(2, s.jobsInState(JobState.PREEMPTED).size());
        Map<String, Integer> usageAfterPreempt = s.usageByQueue();

        // 重复触发多轮调度：被抢占作业不得被重复扣减、不得被重复调度出不同结论
        for (int round = 0; round < 5; round++) {
            s.scheduleOnce();
            assertTrue(s.accountingConsistent(), "第 %d 轮重复调度后账目仍守恒".formatted(round));
        }
        assertEquals(usageAfterPreempt, s.usageByQueue(), "重复触发不得改变资源账目");
        assertEquals(2, s.jobsInState(JobState.PREEMPTED).size()
                + s.jobsInState(JobState.PENDING).size(), "被抢占作业只应排队等待，不得重复运行");
        for (Job j : s.jobsInState(JobState.PREEMPTED)) {
            assertEquals(1, j.preemptionEpoch(), "同一作业只被抢占一次，epoch 不得重复递增");
        }
        s.decisionLog().printAll();
    }

    @Test
    void fairShareConvergesToWeights() {
        Scheduler s = standardCluster();
        // beta 先占满，随后 alpha 持续提交，每次完成一个作业后重排，
        // 长期看运行份额应向权重 2:1 收敛（8 GPU -> alpha 5 / beta 3）
        for (int i = 0; i < 8; i++) {
            s.submit(req("beta-" + i, "beta", 1, 1));
        }
        for (int i = 0; i < 5; i++) {
            s.submit(req("alpha-" + i, "alpha", 1, 1));
        }
        Map<String, Integer> usage = s.usageByQueue();
        Map<String, Integer> shares = s.fairShares();
        System.out.println("[FairShare] usage=" + usage + " shares=" + shares);
        assertEquals(shares.get("alpha"), usage.getOrDefault("alpha", 0),
                "欠账队列应抢占收敛到公平份额");
        assertEquals(shares.get("beta"), usage.getOrDefault("beta", 0));
        assertTrue(s.accountingConsistent());
        s.decisionLog().printAll();
    }
}
