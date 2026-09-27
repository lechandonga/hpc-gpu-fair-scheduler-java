package com.github.lechandonga.hpcgpufairschedulerjava.scheduler.core;

import java.util.Map;

/**
 * 加权公平份额策略：按队列权重计算各队列应得份额，
 * 长期低于份额的队列在调度与抢占选择中获得优先权，避免饿死。
 */
public final class FairSharePolicy {

    /**
     * 计算每个队列的公平份额（GPU 维度）。
     *
     * @param weights 队列名 -> 权重（正数）
     * @param totalGpu 可分配 GPU 总量
     * @return 队列名 -> 应得 GPU 份额（最大剩余法取整，总和等于 totalGpu）
     */
    public Map<String, Integer> fairShares(Map<String, Double> weights, int totalGpu) {
        // TODO: 实现
        return Map.of();
    }

    /**
     * 判断队列是否处于"欠账"状态（当前用量低于公平份额）。
     */
    public boolean isStarving(String queue, Map<String, Double> weights,
                              Map<String, Integer> usage, int totalGpu) {
        // TODO: 实现
        return false;
    }
}
