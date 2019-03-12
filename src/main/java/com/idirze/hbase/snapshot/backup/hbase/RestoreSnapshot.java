package com.idirze.hbase.snapshot.backup.hbase;

import com.idirze.hbase.snapshot.backup.commad.BackupRestoreOperation;
import com.idirze.hbase.snapshot.backup.utils.SnapshotBackupUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.client.Connection;

@Slf4j
public class RestoreSnapshot extends Configured implements BackupRestoreOperation {

    private String tableSnapshotId;
    private String srcTable;
    private String targetTable;
    private Connection connection;

    public RestoreSnapshot(Connection connection, String srcTable, String targetTable, String backupId) {
        this.connection = connection;
        this.tableSnapshotId = SnapshotBackupUtils.tableSnapshotId(backupId, srcTable);
        this.srcTable = srcTable;
        this.targetTable = targetTable;
    }

    @Override
    public void execute() throws Exception {
        log.info("Restore snapshot: {}, for table: {} into target table {}", tableSnapshotId, srcTable, targetTable);
        SnapshotBackupUtils.createNamespaceIfNotExistsForTable(connection, targetTable);
        if (!srcTable.equals(targetTable)) {
            SnapshotBackupUtils.cloneSnapshot(connection, tableSnapshotId, targetTable);
        } else {
            log.info("Restoring the snapshot {} into the source table {}", tableSnapshotId, srcTable);
            SnapshotBackupUtils.restoreSnapshot(connection, tableSnapshotId);
        }
    }

    @Override
    public void rollback() throws Exception {
        log.info("Rollback - Restore snapshot {} - Nothing to do", tableSnapshotId);
    }
}
