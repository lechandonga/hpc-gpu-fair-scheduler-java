package com.github.lechandonga.hpcgpufairschedulerjava.scheduler.model;

/**
 * 队列定义：名称 + 公平共享权重（正整数）。
 */
public record QueueDef(String name, int weight) {
    public QueueDef {
        if (name == null || name.isBlank()) {
            throw new IllegalArgumentException("queue name must not be blank");
        }
        if (weight <= 0) {
            throw new IllegalArgumentException("queue weight must be > 0");
        }
    }
}
