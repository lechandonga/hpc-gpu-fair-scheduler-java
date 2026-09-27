package com.github.lechandonga.hpcgpufairschedulerjava.scheduler.core;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/** 调度器 Bean 装配（本地演示用空集群，测试自行构造）。 */
@Configuration
public class SchedulerConfig {

    @Bean
    public Scheduler scheduler() {
        return new Scheduler();
    }
}
