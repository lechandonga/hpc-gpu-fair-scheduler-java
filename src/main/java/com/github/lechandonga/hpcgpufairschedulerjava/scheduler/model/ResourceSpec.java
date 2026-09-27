package com.github.lechandonga.hpcgpufairschedulerjava.scheduler.model;

/** 资源向量，不可变。 */
public record ResourceSpec(int gpu, int cpuCores, long memoryMb) {

    public static final ResourceSpec ZERO = new ResourceSpec(0, 0, 0L);

    public ResourceSpec plus(ResourceSpec o) {
        return new ResourceSpec(gpu + o.gpu, cpuCores + o.cpuCores, memoryMb + o.memoryMb);
    }

    public ResourceSpec minus(ResourceSpec o) {
        return new ResourceSpec(gpu - o.gpu, cpuCores - o.cpuCores, memoryMb - o.memoryMb);
    }

    public boolean fitsWithin(ResourceSpec capacity) {
        return gpu <= capacity.gpu && cpuCores <= capacity.cpuCores && memoryMb <= capacity.memoryMb;
    }

    public boolean isPositive() {
        return gpu > 0 && cpuCores > 0 && memoryMb > 0;
    }
}
