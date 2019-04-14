package com.idirze.hbase.snapshot.backup.commad.impl;

import com.idirze.hbase.snapshot.backup.admin.CreateSnapshot;
import com.idirze.hbase.snapshot.backup.admin.DeleteSnapshot;
import com.idirze.hbase.snapshot.backup.cli.CreateBackupOptions;
import com.idirze.hbase.snapshot.backup.cli.RollupBackupOptions;
import com.idirze.hbase.snapshot.backup.commad.BackupCommand;
import com.idirze.hbase.snapshot.backup.manifest.BackupManifest;
import com.idirze.hbase.snapshot.backup.manifest.BackupManifests;
import com.idirze.hbase.snapshot.backup.stats.Stats;
import com.idirze.hbase.snapshot.backup.utils.FileUtils;
import com.idirze.hbase.snapshot.backup.utils.SnapshotBackupUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;

import java.util.List;
import java.util.stream.Collectors;

@Slf4j
public class BackupImpl extends Configured {

    public BackupImpl(Configuration conf) {
        setConf(conf);
    }

    public int backup(Connection conn,
                      String backupId,
                      CreateBackupOptions backupOpts) throws Exception {
        int exit;

        String backupManifestPath = FileUtils.path(backupOpts.getBackupRooPath(), BackupManifest.BACKUP_MANIFEST_NAME);

        BackupManifests backupManifest = BackupManifests.readFrom(getConf(), backupManifestPath);

        List<TableName> tableNames = null;
        if (backupOpts.getNamespaces() != null && !backupOpts.getNamespaces().isEmpty()) {
            log.info("Performing backup for namespaces: {}", backupOpts.getNamespaces());
            tableNames = SnapshotBackupUtils.listTableNamesByNamespaces(conn, backupOpts.getNamespaces());
        } else if (backupOpts.getTables() != null && !backupOpts.getTables().isEmpty()) {
            log.info("Performing backup for tables: {}", backupOpts.getNamespaces());
            tableNames = SnapshotBackupUtils.listTableNames(conn, backupOpts.getTables());
        } else {
            log.error("-namespaces or -table should be provided");
            System.exit(-1);
        }


        log.info("Found tables: {}", tableNames.stream().map(t -> t.getNameAsString()).collect(Collectors.toList()));

        HbaseBackupRestoreTables createSnapshots = new HbaseBackupRestoreTables(3);
        for (TableName tableName : tableNames) {
            HbaseBackupRestoreTable bkRestoreTable = new HbaseBackupRestoreTable();
            bkRestoreTable
                    .addOperation(new CreateSnapshot(conn, tableName.getNameAsString(), backupId));
            createSnapshots.add(bkRestoreTable);
        }

        // createSnapshots.submit(); Hbase snapshot creation is sequential
        exit = createSnapshots.execute();
        Stats.log();
        log.info("Create snapshot exit status: {}", exit);
        if (exit != 0) return exit;

        HbaseBackupRestoreTables backupRestoreTables = new HbaseBackupRestoreTables(3);
        for (TableName tableName : tableNames) {
            HbaseBackupRestoreTable bkRestoreTable = new HbaseBackupRestoreTable();
            bkRestoreTable
                    // .addOperation(new CreateSnapshot(conn, tableName.getNameAsString(), backupId))
                    .addOperation(new DoExportSnapshot(getConf(), tableName.getNameAsString(), backupId, backupManifest, backupOpts));
            // .addOperation(new DeleteSnapshot(conn, tableName.getNameAsString(), backupId));

            backupRestoreTables.add(bkRestoreTable);
        }

        exit = backupRestoreTables.execute();
        log.info("Export snapshot exit status: {}", exit);
        if (exit != 0) return exit;

        HbaseBackupRestoreTables deleteSnapshots = new HbaseBackupRestoreTables(3);
        for (TableName tableName : tableNames) {
            HbaseBackupRestoreTable bkRestoreTable = new HbaseBackupRestoreTable();
            bkRestoreTable
                    .addOperation(new DeleteSnapshot(conn, tableName.getNameAsString(), backupId));
            deleteSnapshots.add(bkRestoreTable);
        }

        exit = deleteSnapshots.execute();
        if (exit != 0) return exit;
        log.info("Delete snapshot exit status: {}", exit);

        if (backupOpts.getRollup() != -1) {

            RollupBackupOptions rollupOpts = new RollupBackupOptions();
            rollupOpts.setBackupRooPath(backupOpts.getBackupRooPath());
            rollupOpts.setCommand(BackupCommand.ROLLUP);
            rollupOpts.setNbBackups(backupOpts.getRollup());

            exit = new HbaseBackupRestoreTable()
                    .addOperation(new RollupBackup(getConf(), backupManifest, rollupOpts))
                    .execute();

            if (exit != 0) return exit;
        }

        return 0;
    }
}
