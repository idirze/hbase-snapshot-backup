package com.idirze.hbase.snapshot.backup.admin;

import com.idirze.hbase.snapshot.backup.commad.BackupRestoreOperation;
import com.idirze.hbase.snapshot.backup.stats.Stats;
import com.idirze.hbase.snapshot.backup.stats.Stats.StatType;
import com.idirze.hbase.snapshot.backup.utils.SnapshotBackupUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.client.Connection;

import java.time.Instant;

import static org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.SnapshotDescription.Type.FLUSH;

@Slf4j
public class CreateSnapshot extends Configured implements BackupRestoreOperation {

    private String tableName;
    private String tableSnapshotId;
    private Connection connection;

    public CreateSnapshot(Connection connection, String tableName, String backupId) {
        this.connection = connection;
        this.tableName = tableName;
        this.tableSnapshotId = SnapshotBackupUtils.tableSnapshotId(backupId, tableName);

    }

    @Override
    public void execute() throws Exception {
        Instant start = Instant.now();
        log.info("Create snapshot: {} for table: {}", tableSnapshotId, tableName);
        SnapshotBackupUtils.createSnapshot(connection, tableName, tableSnapshotId, FLUSH.name());
        Instant finish = Instant.now();
        Stats.Stat s = new Stats.Stat(tableSnapshotId, tableName, StatType.CREATE_SNAPSHOT, start, finish);
        Stats.add(s);
        Stats.log(s);
    }

    @Override
    public void rollback() throws Exception {
        log.info("Rollback - Delete snapshot: {} for the table: {}", tableSnapshotId, tableName);
       // SnapshotBackupUtils.deleteSnapshot(connection, tableSnapshotId);
    }
}
