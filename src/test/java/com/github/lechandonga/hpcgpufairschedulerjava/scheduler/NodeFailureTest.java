package com.github.lechandonga.hpcgpufairschedulerjava.scheduler;

import com.github.lechandonga.hpcgpufairschedulerjava.scheduler.core.Scheduler;
import com.github.lechandonga.hpcgpufairschedulerjava.scheduler.model.Job;
import com.github.lechandonga.hpcgpufairschedulerjava.scheduler.model.JobState;
import com.github.lechandonga.hpcgpufairschedulerjava.scheduler.model.NodeStatus;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;

import static com.github.lechandonga.hpcgpufairschedulerjava.scheduler.SchedulerTestSupport.req;
import static com.github.lechandonga.hpcgpufairschedulerjava.scheduler.SchedulerTestSupport.standardCluster;
import static org.junit.jupiter.api.Assertions.*;

/** 节点故障：下线/失联/恢复时作业处置明确、无超卖、结论可复现。 */
class NodeFailureTest {

    @Test
    void nodeDownEvictsAndReschedulesWithoutOversell() {
        Scheduler s = standardCluster();
        // node-1 上放 4 个 1-GPU 作业（first-fit 先填满 node-1）
        for (int i = 0; i < 4; i++) {
            s.submit(req("j" + i, "alpha", 1, 1));
        }
        assertEquals(4, s.nodes().get("node-1").allocated().gpu());

        // node-1 失联：其上作业应被逐出并重排到 node-2
        s.onNodeStatusChange("node-1", NodeStatus.UNREACHABLE);
        assertEquals(0, s.nodes().get("node-1").allocated().gpu(), "故障节点账目必须清零");
        List<Job> running = s.jobsInState(JobState.RUNNING);
        assertEquals(4, running.size(), "作业应全部重排到存活节点");
        assertTrue(running.stream().allMatch(j -> "node-2".equals(j.nodeId())));
        assertEquals(4, s.nodes().get("node-2").allocated().gpu());
        assertTrue(s.accountingConsistent(), "故障转移后账目守恒、无超卖");
        s.decisionLog().printAll();
    }

    @Test
    void nodeDownWithInsufficientRemainingCapacityKeepsJobsQueued() {
        Scheduler s = standardCluster();
        // 占满两节点共 8 GPU
        for (int i = 0; i < 8; i++) {
            s.submit(req("j" + i, "alpha", 1, 1));
        }
        // node-1 下线：4 个作业失去归宿，剩余容量不足 -> 明确排队等待而非悬空
        s.onNodeStatusChange("node-1", NodeStatus.DOWN);
        assertEquals(4, s.jobsInState(JobState.RUNNING).size());
        assertEquals(4, s.jobsInState(JobState.PENDING).size());
        assertTrue(s.jobsInState(JobState.PENDING).stream().allMatch(j -> j.nodeId() == null));
        assertTrue(s.accountingConsistent());
        s.decisionLog().printAll();
    }

    @Test
    void recoveryReschedulingIsReproducible() {
        // 同一脚本执行两次，最终放置结论必须完全一致
        Map<String, String> run1 = runFailureRecoveryScript();
        Map<String, String> run2 = runFailureRecoveryScript();
        assertEquals(run1, run2, "相同事件序列的调度结论必须可复现");
    }

    private Map<String, String> runFailureRecoveryScript() {
        Scheduler s = standardCluster();
        for (int i = 0; i < 8; i++) {
            s.submit(req("j" + i, i % 2 == 0 ? "alpha" : "beta", 1, 1));
        }
        s.onNodeStatusChange("node-1", NodeStatus.DOWN);
        s.onNodeStatusChange("node-1", NodeStatus.UP); // 恢复后重排
        Map<String, String> placement = s.jobsInState(JobState.RUNNING).stream()
                .collect(Collectors.toMap(Job::jobId, Job::nodeId, (a, b) -> a, TreeMap::new));
        System.out.println("[Recovery] placement=" + placement
                + " pending=" + s.jobsInState(JobState.PENDING).stream().map(Job::jobId).sorted().toList());
        assertTrue(s.accountingConsistent());
        return placement;
    }

    @Test
    void recoveredNodeAcceptsNewWork() {
        Scheduler s = standardCluster();
        for (int i = 0; i < 8; i++) {
            s.submit(req("j" + i, "alpha", 1, 1));
        }
        s.onNodeStatusChange("node-2", NodeStatus.DOWN);
        assertEquals(4, s.jobsInState(JobState.PENDING).size());
        // 节点恢复：排队作业应被确定性地补调度
        s.onNodeStatusChange("node-2", NodeStatus.UP);
        assertEquals(8, s.jobsInState(JobState.RUNNING).size());
        assertEquals(4, s.nodes().get("node-2").allocated().gpu());
        assertTrue(s.accountingConsistent());
        s.decisionLog().printAll();
    }
}
