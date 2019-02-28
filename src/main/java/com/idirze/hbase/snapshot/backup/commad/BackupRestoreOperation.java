package com.idirze.hbase.snapshot.backup.commad;

public interface BackupRestoreOperation {

    void execute() throws Exception;

    void rollback() throws Exception;
}
