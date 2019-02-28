package com.idirze.hbase.snapshot.backup.hbase;

import com.idirze.hbase.snapshot.backup.commad.BackupRestoreOperation;
import com.idirze.hbase.snapshot.backup.utils.SnapshotBackupUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.client.Connection;

@Slf4j
public class DeleteSnapshot extends Configured implements BackupRestoreOperation {

    private String tableSnapshotId;
    private String tableName;
    private Connection connection;

    public DeleteSnapshot(Connection connection, String tableName, String backupId) {
        this.connection = connection;
        this.tableName = tableName;
        this.tableSnapshotId = SnapshotBackupUtils.tableSnapshotId(backupId, tableName);

    }

    @Override
    public void execute() throws Exception {
        log.info("Delete snapshot: {} for table: {}", tableSnapshotId, tableName);
        SnapshotBackupUtils.deleteSnapshot(connection, tableSnapshotId);
    }

    @Override
    public void rollback() {
        log.info("Rollback - Delete snapshot {} for table {} - Nothing to do", tableSnapshotId, tableName);
    }
}
