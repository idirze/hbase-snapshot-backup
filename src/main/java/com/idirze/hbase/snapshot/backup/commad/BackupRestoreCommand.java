package com.idirze.hbase.snapshot.backup.commad;

public interface BackupRestoreCommand {

     void execute() throws Exception;
     void rollback() throws Exception;
}
