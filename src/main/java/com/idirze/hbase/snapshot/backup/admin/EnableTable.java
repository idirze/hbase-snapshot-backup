package com.idirze.hbase.snapshot.backup.admin;

import com.idirze.hbase.snapshot.backup.commad.BackupRestoreOperation;
import com.idirze.hbase.snapshot.backup.utils.SnapshotBackupUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.client.Connection;

@Slf4j
public class EnableTable extends Configured implements BackupRestoreOperation {

    private String tableName;
    private Connection connection;

    public EnableTable(Connection connection, String tableName) {
        this.connection = connection;
        this.tableName = tableName;
    }

    @Override
    public void execute() throws Exception {
        log.info("Enable table: {}", tableName);
        SnapshotBackupUtils.enableTable(connection, tableName);
    }

    @Override
    public void rollback() throws Exception {
        log.info("Rollback - Enable table {} - Nothing to do", tableName);
    }
}
