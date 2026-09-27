package com.github.lechandonga.hpcgpufairschedulerjava.scheduler.core;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 加权公平份额策略：按队列权重计算各队列应得份额，
 * 长期低于份额的队列在调度与抢占选择中获得优先权，避免饿死。
 *
 * 取整采用最大剩余法（largest remainder），保证各队列份额之和严格等于总量，
 * 且结果只依赖输入的排序后键集合，与遍历顺序无关（确定性、可复现）。
 */
public final class FairSharePolicy {

    /**
     * 计算每个队列的公平份额（GPU 维度）。
     *
     * @param weights  队列名 -> 权重（正数）
     * @param totalGpu 可分配 GPU 总量
     * @return 队列名 -> 应得 GPU 份额，总和等于 totalGpu
     */
    public Map<String, Integer> fairShares(Map<String, Double> weights, int totalGpu) {
        Map<String, Integer> shares = new HashMap<>();
        if (weights.isEmpty() || totalGpu <= 0) {
            weights.keySet().forEach(q -> shares.put(q, 0));
            return shares;
        }
        double weightSum = weights.values().stream().mapToDouble(Double::doubleValue).sum();
        // 先取地板，再按小数余量从大到小分配剩余份额；余量相同按队列名字典序，保证确定性。
        record Remainder(String queue, double frac) {}
        List<Remainder> remainders = new ArrayList<>();
        int assigned = 0;
        for (Map.Entry<String, Double> e : weights.entrySet()) {
            double exact = totalGpu * e.getValue() / weightSum;
            int floor = (int) Math.floor(exact);
            shares.put(e.getKey(), floor);
            assigned += floor;
            remainders.add(new Remainder(e.getKey(), exact - floor));
        }
        remainders.sort(Comparator.comparingDouble(Remainder::frac).reversed()
                .thenComparing(Remainder::queue));
        int leftover = totalGpu - assigned;
        for (int i = 0; i < leftover; i++) {
            String q = remainders.get(i % remainders.size()).queue();
            shares.merge(q, 1, Integer::sum);
        }
        return shares;
    }

    /**
     * 判断队列是否处于"欠账"状态：当前 GPU 用量低于其公平份额。
     */
    public boolean isStarving(String queue, Map<String, Double> weights,
                              Map<String, Integer> usage, int totalGpu) {
        Map<String, Integer> shares = fairShares(weights, totalGpu);
        return usage.getOrDefault(queue, 0) < shares.getOrDefault(queue, 0);
    }
}
