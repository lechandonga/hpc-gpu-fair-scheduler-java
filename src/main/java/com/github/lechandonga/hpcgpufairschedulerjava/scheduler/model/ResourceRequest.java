package com.github.lechandonga.hpcgpufairschedulerjava.scheduler.model;

/**
 * 一份资源需求 / 容量。不可变，提供加减与可容纳判断。
 */
public record ResourceRequest(int cpuCores, int gpus, long memoryMb) {

    public static final ResourceRequest ZERO = new ResourceRequest(0, 0, 0);

    public ResourceRequest {
        if (cpuCores < 0 || gpus < 0 || memoryMb < 0) {
            throw new IllegalArgumentException("resource dimensions must be >= 0");
        }
    }

    public ResourceRequest plus(ResourceRequest other) {
        return new ResourceRequest(cpuCores + other.cpuCores,
                gpus + other.gpus, memoryMb + other.memoryMb);
    }

    public ResourceRequest minus(ResourceRequest other) {
        return new ResourceRequest(cpuCores - other.cpuCores,
                gpus - other.gpus, memoryMb - other.memoryMb);
    }

    /** this 是否能容纳 need（每个维度都 >=）。 */
    public boolean canFit(ResourceRequest need) {
        return cpuCores >= need.cpuCores && gpus >= need.gpus && memoryMb >= need.memoryMb;
    }

    public boolean isZero() {
        return cpuCores == 0 && gpus == 0 && memoryMb == 0;
    }

    @Override
    public String toString() {
        return "[cpu=" + cpuCores + ", gpu=" + gpus + ", memMb=" + memoryMb + "]";
    }
}
