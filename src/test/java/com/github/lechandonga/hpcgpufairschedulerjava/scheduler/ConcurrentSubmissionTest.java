package com.github.lechandonga.hpcgpufairschedulerjava.scheduler;

import com.github.lechandonga.hpcgpufairschedulerjava.scheduler.core.Scheduler;
import com.github.lechandonga.hpcgpufairschedulerjava.scheduler.model.Job;
import com.github.lechandonga.hpcgpufairschedulerjava.scheduler.model.JobState;
import com.github.lechandonga.hpcgpufairschedulerjava.scheduler.model.RejectionReason;
import com.github.lechandonga.hpcgpufairschedulerjava.scheduler.model.SubmitResult;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static com.github.lechandonga.hpcgpufairschedulerjava.scheduler.SchedulerTestSupport.req;
import static com.github.lechandonga.hpcgpufairschedulerjava.scheduler.SchedulerTestSupport.standardCluster;
import static org.junit.jupiter.api.Assertions.*;

/** 并发提交：账目守恒、无超卖、重复提交恰好被受理一次。 */
class ConcurrentSubmissionTest {

    @Test
    void concurrentDistinctSubmissionsKeepAccountingConsistent() throws Exception {
        Scheduler s = standardCluster(); // 共 8 GPU
        int threads = 16, perThread = 8;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        List<Future<SubmitResult>> futures = new ArrayList<>();
        for (int t = 0; t < threads; t++) {
            int base = t * perThread;
            for (int i = 0; i < perThread; i++) {
                String id = "job-" + (base + i);
                String queue = (base + i) % 2 == 0 ? "alpha" : "beta";
                futures.add(pool.submit(() -> s.submit(req(id, queue, 1, 1))));
            }
        }
        int accepted = 0;
        for (Future<SubmitResult> f : futures) {
            if (f.get().accepted()) accepted++;
        }
        pool.shutdown();
        assertEquals(threads * perThread, accepted, "全部唯一作业都应被受理（排队或运行）");

        // 8 GPU 总量，每个作业 1 GPU：运行数不得超过 8，其余排队
        List<Job> running = s.jobsInState(JobState.RUNNING);
        assertTrue(running.size() <= 8, "运行作业数不得超过集群 GPU 总量");
        assertEquals(accepted, running.size() + s.jobsInState(JobState.PENDING).size()
                + s.jobsInState(JobState.PREEMPTED).size());
        assertTrue(s.accountingConsistent(), "并发提交后账目必须守恒");
        System.out.println("[ConcurrentTest] running=" + running.size()
                + " pending=" + s.jobsInState(JobState.PENDING).size());
        s.decisionLog().printAll();
    }

    @Test
    void concurrentDuplicateSubmissionAcceptedExactlyOnce() throws Exception {
        Scheduler s = standardCluster();
        int threads = 32;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        ConcurrentLinkedQueue<SubmitResult> results = new ConcurrentLinkedQueue<>();
        List<Future<?>> futures = new ArrayList<>();
        for (int i = 0; i < threads; i++) {
            futures.add(pool.submit((Callable<Void>) () -> {
                results.add(s.submit(req("same-job", "alpha", 1, 1)));
                return null;
            }));
        }
        for (Future<?> f : futures) f.get();
        pool.shutdown();

        long acceptedCount = results.stream().filter(SubmitResult::accepted).count();
        long dupCount = results.stream()
                .filter(r -> !r.accepted() && r.reason() == RejectionReason.DUPLICATE_JOB).count();
        assertEquals(1, acceptedCount, "同一 jobId 并发提交只能受理一次");
        assertEquals(threads - 1, dupCount, "其余提交必须以 DUPLICATE_JOB 拒绝");
        assertTrue(s.accountingConsistent());
        System.out.println("[ConcurrentTest] accepted=" + acceptedCount + " duplicates=" + dupCount);
        s.decisionLog().printAll();
    }
}
