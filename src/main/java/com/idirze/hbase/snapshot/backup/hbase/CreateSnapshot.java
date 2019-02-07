package com.idirze.hbase.snapshot.backup.hbase;

import com.idirze.hbase.snapshot.backup.commad.BackupRestoreCommand;
import com.idirze.hbase.snapshot.backup.utils.SnapshotBackupUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;

import static org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.SnapshotDescription.Type.FLUSH;

@Slf4j
public class CreateSnapshot extends Configured implements BackupRestoreCommand {

    private String tableName;
    private String tableSnapshotId;

    public CreateSnapshot(Configuration conf, String tableName, String backupId) {
        setConf(conf);
        this.tableName = tableName;
        this.tableSnapshotId = SnapshotBackupUtils.tableSnapshotId(backupId, tableName);

    }

    @Override
    public void execute() throws Exception {
        log.info("Create snapshot: {} for table: {}", tableSnapshotId, tableName);
        SnapshotBackupUtils.createSnapshot(getConf(), tableName, tableSnapshotId, FLUSH.name());
    }

    @Override
    public void rollback() throws Exception {
        log.info("Rollback - Delete snapshot: {} for the table: {}", tableSnapshotId, tableName);
        SnapshotBackupUtils.deleteSnapshot(getConf(), tableSnapshotId);
    }
}
