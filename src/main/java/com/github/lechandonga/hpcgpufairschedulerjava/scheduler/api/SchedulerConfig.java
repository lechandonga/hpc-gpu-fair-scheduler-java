package com.github.lechandonga.hpcgpufairschedulerjava.scheduler.api;

import com.github.lechandonga.hpcgpufairschedulerjava.scheduler.core.FairScheduler;
import com.github.lechandonga.hpcgpufairschedulerjava.scheduler.model.Node;
import com.github.lechandonga.hpcgpufairschedulerjava.scheduler.model.QueueDef;
import com.github.lechandonga.hpcgpufairschedulerjava.scheduler.model.ResourceRequest;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.List;

/**
 * 本地模拟集群：2 个节点（各 8 GPU / 32 核 / 256GB），3 条权重不同的队列。
 */
@Configuration
public class SchedulerConfig {

    @Bean
    public FairScheduler fairScheduler() {
        List<Node> nodes = List.of(
                new Node("node-1", new ResourceRequest(32, 8, 256_000)),
                new Node("node-2", new ResourceRequest(32, 8, 256_000)));
        List<QueueDef> queues = List.of(
                new QueueDef("research", 3),
                new QueueDef("teaching", 2),
                new QueueDef("batch", 1));
        return new FairScheduler(nodes, queues);
    }
}
