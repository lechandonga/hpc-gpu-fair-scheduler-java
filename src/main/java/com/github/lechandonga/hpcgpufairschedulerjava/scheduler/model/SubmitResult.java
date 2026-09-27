package com.github.lechandonga.hpcgpufairschedulerjava.scheduler.model;

/** 提交结果：受理或带明确原因的拒绝。 */
public record SubmitResult(boolean accepted, RejectionReason reason, String detail) {

    public static SubmitResult ok() {
        return new SubmitResult(true, null, "accepted");
    }

    public static SubmitResult rejected(RejectionReason reason, String detail) {
        return new SubmitResult(false, reason, detail);
    }
}
