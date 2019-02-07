package com.idirze.hbase.snapshot.backup.commad;

public enum BackupStatus {

    FAILED,
    SUCCESS;

    public boolean isSuccess() {
        return this == SUCCESS;
    }

    public boolean isFailed() {
        return this == FAILED;
    }
}
