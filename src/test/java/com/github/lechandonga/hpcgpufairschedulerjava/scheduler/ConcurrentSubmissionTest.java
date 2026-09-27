package com.github.lechandonga.hpcgpufairschedulerjava.scheduler;

import com.github.lechandonga.hpcgpufairschedulerjava.scheduler.core.FairScheduler;
import com.github.lechandonga.hpcgpufairschedulerjava.scheduler.exception.SchedulingException;
import com.github.lechandonga.hpcgpufairschedulerjava.scheduler.exception.SchedulingException.Reason;
import com.github.lechandonga.hpcgpufairschedulerjava.scheduler.model.*;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 并发提交：多线程同时提交不同 / 相同 jobId，
 * 结论必须确定——每个 jobId 最多被接受一次，资源不超卖，账目守恒。
 */
class ConcurrentSubmissionTest {

    private FairScheduler scheduler;

    @AfterEach
    void printDecisions() {
        System.out.println("---- decision log (last 30) ----");
        List<DecisionRecord> log = scheduler.getDecisionLog();
        log.stream().skip(Math.max(0, log.size() - 30)).forEach(System.out::println);
    }

    @Test
    void concurrentDistinctSubmissionsAllAcceptedExactlyOnce() throws Exception {
        scheduler = new FairScheduler(
                List.of(new Node("node-1", new ResourceRequest(64, 16, 256_000)),
                        new Node("node-2", new ResourceRequest(64, 16, 256_000))),
                List.of(new QueueDef("a", 1), new QueueDef("b", 1)));

        int threads = 16, jobsPerThread = 8;
        AtomicInteger accepted = new AtomicInteger();
        AtomicInteger rejected = new AtomicInteger();

        runConcurrently(threads, jobsPerThread, (t, i) -> {
            try {
                scheduler.submit(new Job("job-" + t + "-" + i, t % 2 == 0 ? "a" : "b",
                        0, new ResourceRequest(1, 1, 100)));
                accepted.incrementAndGet();
            } catch (SchedulingException e) {
                rejected.incrementAndGet();
            }
        });

        int total = threads * jobsPerThread;
        assertEquals(total, accepted.get(), "distinct jobIds must all be accepted");
        assertEquals(0, rejected.get());
        // 每个作业在调度器里恰好存在一份
        for (int t = 0; t < threads; t++) {
            for (int i = 0; i < jobsPerThread; i++) {
                assertNotNull(scheduler.getJob("job-" + t + "-" + i));
            }
        }
        assertNoOversellAndBalanced();
    }

    @Test
    void concurrentDuplicateSubmissionsAcceptedExactlyOnce() throws Exception {
        scheduler = new FairScheduler(
                List.of(new Node("node-1", new ResourceRequest(64, 16, 256_000))),
                List.of(new QueueDef("a", 1)));

        int threads = 32;
        AtomicInteger accepted = new AtomicInteger();
        AtomicInteger duplicates = new AtomicInteger();

        // 32 个线程同时提交同一个 jobId
        runConcurrently(threads, 1, (t, i) -> {
            try {
                scheduler.submit(new Job("same-id", "a", 0, new ResourceRequest(1, 1, 100)));
                accepted.incrementAndGet();
            } catch (SchedulingException e) {
                assertEquals(Reason.DUPLICATE_JOB, e.getReason());
                duplicates.incrementAndGet();
            }
        });

        assertEquals(1, accepted.get(), "exactly one submission may win");
        assertEquals(threads - 1, duplicates.get());
        assertEquals(1, scheduler.getNodes().get("node-1").getAllocated().gpus(),
                "resource must be deducted exactly once");
        assertNoOversellAndBalanced();
    }

    @Test
    void concurrentContendedSubmissionsNeverOversell() throws Exception {
        // 只有 8 GPU，64 个线程各要 1 GPU：最多 8 个 RUNNING，其余排队
        scheduler = new FairScheduler(
                List.of(new Node("node-1", new ResourceRequest(64, 8, 256_000))),
                List.of(new QueueDef("a", 1)));

        runConcurrently(64, 1, (t, i) ->
                scheduler.submit(new Job("c-" + t, "a", 0, new ResourceRequest(1, 1, 100))));

        long running = countByStatus(JobStatus.RUNNING);
        long pending = countByStatus(JobStatus.PENDING);
        System.out.println("running=" + running + ", pending=" + pending);
        assertEquals(8, running);
        assertEquals(56, pending);
        assertNoOversellAndBalanced();
    }

    // ---------------- helpers ----------------

    private void runConcurrently(int threads, int jobsPerThread, JobSubmitter submitter)
            throws InterruptedException {
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch ready = new CountDownLatch(threads);
        CountDownLatch go = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(threads);
        for (int t = 0; t < threads; t++) {
            final int threadIdx = t;
            pool.submit(() -> {
                ready.countDown();
                try {
                    go.await();
                    for (int i = 0; i < jobsPerThread; i++) {
                        submitter.submit(threadIdx, i);
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    done.countDown();
                }
            });
        }
        ready.await();
        go.countDown(); // 所有线程同时放行，最大化竞争
        assertTrue(done.await(30, TimeUnit.SECONDS), "submissions must finish");
        pool.shutdownNow();
    }

    @FunctionalInterface
    interface JobSubmitter {
        void submit(int threadIdx, int i);
    }

    private long countByStatus(JobStatus status) {
        return scheduler.getJobs().stream().filter(j -> j.getStatus() == status).count();
    }

    private void assertNoOversellAndBalanced() {
        for (Node node : scheduler.getNodes().values()) {
            assertTrue(node.getCapacity().canFit(node.getAllocated()),
                    "node " + node.getNodeId() + " oversold: " + node.getAllocated()
                            + " > " + node.getCapacity());
        }
        assertTrue(scheduler.accountingBalanced(), "node-side and job-side accounting diverge");
    }
}
