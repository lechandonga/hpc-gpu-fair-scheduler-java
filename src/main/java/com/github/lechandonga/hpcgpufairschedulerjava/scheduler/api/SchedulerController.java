package com.github.lechandonga.hpcgpufairschedulerjava.scheduler.api;

import com.github.lechandonga.hpcgpufairschedulerjava.scheduler.core.Scheduler;
import com.github.lechandonga.hpcgpufairschedulerjava.scheduler.model.JobRequest;
import com.github.lechandonga.hpcgpufairschedulerjava.scheduler.model.SubmitResult;
import org.springframework.web.bind.annotation.*;

/** REST 入口（本地验证用）。 */
@RestController
@RequestMapping("/api/scheduler")
public class SchedulerController {

    private final Scheduler scheduler;

    public SchedulerController(Scheduler scheduler) {
        this.scheduler = scheduler;
    }

    @PostMapping("/jobs")
    public SubmitResult submit(@RequestBody JobRequest request) {
        return scheduler.submit(request);
    }
}
