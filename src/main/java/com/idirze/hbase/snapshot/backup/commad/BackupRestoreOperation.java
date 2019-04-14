package com.idirze.hbase.snapshot.backup.commad;

public interface BackupRestoreOperation {

    void execute() throws Exception;

    default void rollback() throws Exception {

    }
}
