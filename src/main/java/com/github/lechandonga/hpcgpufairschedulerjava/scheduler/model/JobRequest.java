package com.github.lechandonga.hpcgpufairschedulerjava.scheduler.model;

/**
 * 作业提交参数。
 *
 * @param jobId     作业唯一标识，重复提交按 {@link RejectionReason#DUPLICATE_JOB} 拒绝
 * @param queue     目标队列名
 * @param priority  队列内优先级，数值越大越优先
 * @param resources 全有或全无的资源需求
 */
public record JobRequest(String jobId, String queue, int priority, ResourceSpec resources) {
}
