package com.github.lechandonga.hpcgpufairschedulerjava.scheduler.core;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/** 调度决策日志：记录每次决策的依据，供测试断言与排查。 */
public final class DecisionLog {

    public record Entry(String action, String jobId, String rationale) {
        @Override public String toString() {
            return "[%s] job=%s :: %s".formatted(action, jobId, rationale);
        }
    }

    private final List<Entry> entries = new CopyOnWriteArrayList<>();

    public void log(String action, String jobId, String rationale) {
        entries.add(new Entry(action, jobId, rationale));
    }

    public List<Entry> entries() {
        return List.copyOf(entries);
    }

    public void printAll() {
        entries.forEach(e -> System.out.println("[DecisionLog] " + e));
    }
}
