package com.github.lechandonga.hpcgpufairschedulerjava.scheduler.api;

import com.github.lechandonga.hpcgpufairschedulerjava.scheduler.core.FairScheduler;
import com.github.lechandonga.hpcgpufairschedulerjava.scheduler.exception.SchedulingException;
import com.github.lechandonga.hpcgpufairschedulerjava.scheduler.model.*;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/scheduler")
public class SchedulerController {

    private final FairScheduler scheduler;

    public SchedulerController(FairScheduler scheduler) {
        this.scheduler = scheduler;
    }

    public record SubmitRequest(String jobId, String queue, int priority,
                                int cpuCores, int gpus, long memoryMb) {}

    @PostMapping("/jobs")
    public ResponseEntity<?> submit(@RequestBody SubmitRequest req) {
        scheduler.submit(new Job(req.jobId(), req.queue(), req.priority(),
                new ResourceRequest(req.cpuCores(), req.gpus(), req.memoryMb())));
        return ResponseEntity.ok(Map.of("jobId", req.jobId(), "status",
                scheduler.getJob(req.jobId()).getStatus().name()));
    }

    @PostMapping("/jobs/{jobId}/complete")
    public Map<String, String> complete(@PathVariable String jobId) {
        scheduler.complete(jobId);
        return Map.of("jobId", jobId, "status", "COMPLETED");
    }

    @PostMapping("/nodes/{nodeId}/down")
    public Map<String, String> nodeDown(@PathVariable String nodeId) {
        scheduler.nodeDown(nodeId);
        return Map.of("nodeId", nodeId, "status", "DOWN");
    }

    @PostMapping("/nodes/{nodeId}/recover")
    public Map<String, String> nodeRecover(@PathVariable String nodeId) {
        scheduler.nodeRecover(nodeId);
        return Map.of("nodeId", nodeId, "status", "UP");
    }

    @GetMapping("/jobs/{jobId}")
    public Map<String, Object> job(@PathVariable String jobId) {
        Job j = scheduler.getJob(jobId);
        return Map.of("jobId", j.getJobId(), "queue", j.getQueueName(),
                "priority", j.getPriority(), "status", j.getStatus().name(),
                "node", j.getAssignedNodeId() == null ? "" : j.getAssignedNodeId(),
                "preemptions", j.getPreemptionCount());
    }

    @GetMapping("/decisions")
    public List<String> decisions() {
        return scheduler.getDecisionLog().stream().map(DecisionRecord::toString).toList();
    }

    @ExceptionHandler(SchedulingException.class)
    public ResponseEntity<Map<String, String>> onSchedulingError(SchedulingException e) {
        HttpStatus status = switch (e.getReason()) {
            case DUPLICATE_JOB -> HttpStatus.CONFLICT;
            case INVALID_REQUEST, INSUFFICIENT_RESOURCE -> HttpStatus.BAD_REQUEST;
            case UNKNOWN_JOB, UNKNOWN_NODE, UNKNOWN_QUEUE -> HttpStatus.NOT_FOUND;
            case ILLEGAL_STATE -> HttpStatus.CONFLICT;
        };
        return ResponseEntity.status(status)
                .body(Map.of("reason", e.getReason().name(), "message", e.getMessage()));
    }
}
