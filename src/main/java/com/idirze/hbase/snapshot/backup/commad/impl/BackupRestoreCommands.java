package com.idirze.hbase.snapshot.backup.commad.impl;

import com.idirze.hbase.snapshot.backup.cli.CreateBackupOptions;
import com.idirze.hbase.snapshot.backup.cli.HistoryBackupOptions;
import com.idirze.hbase.snapshot.backup.cli.RestoreBackupOptions;
import com.idirze.hbase.snapshot.backup.cli.RollupBackupOptions;
import com.idirze.hbase.snapshot.backup.commad.BackupCommand;
import com.idirze.hbase.snapshot.backup.hbase.*;
import com.idirze.hbase.snapshot.backup.manifest.BackupManifest;
import com.idirze.hbase.snapshot.backup.manifest.BackupManifests;
import com.idirze.hbase.snapshot.backup.utils.FileUtils;
import com.idirze.hbase.snapshot.backup.utils.SnapshotBackupUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.util.Tool;
import picocli.CommandLine;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

@Slf4j
public class BackupRestoreCommands extends Configured implements Tool {

    public static final String BACKUPID_PREFIX = "its_backup_";

    @Override
    public int run(String[] args) throws Exception {

        String command = args.length == 0 ? "-h" : args[0];
        String backupManifestPath;
        BackupManifests backupManifest;
        int exit;
        switch (BackupCommand.backupCommandOf(command)) {
            case CREATE:
                try (final Connection conn = ConnectionFactory.createConnection(getConf())) {
                    CreateBackupOptions backupOpts = parseArgs(new CreateBackupOptions(), args);
                    String backupId = BACKUPID_PREFIX + EnvironmentEdgeManager.currentTime();

                    backupManifestPath = FileUtils.path(backupOpts.getBackupRooPath(), BackupManifest.BACKUP_MANIFEST_NAME);

                    backupManifest = BackupManifests.readFrom(getConf(), backupManifestPath);

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

                    for (TableName tableName : tableNames) {
                        HbaseBackupRestoreOperations operations = new HbaseBackupRestoreOperations();
                        operations.addOperation(new CreateSnapshot(conn, tableName.getNameAsString(), backupId))
                                .addOperation(new CreateBackup(getConf(), tableName.getNameAsString(), backupId, backupManifest, backupOpts))
                                .addOperation(new DeleteSnapshot(conn, tableName.getNameAsString(), backupId));

                        exit = operations.execute();
                        if (exit != 0) return exit;
                    }

                    if (backupOpts.getRollup() != -1) {
                        HbaseBackupRestoreOperations operations = new HbaseBackupRestoreOperations();
                        RollupBackupOptions rollupOpts = new RollupBackupOptions();
                        rollupOpts.setBackupRooPath(backupOpts.getBackupRooPath());
                        rollupOpts.setCommand(BackupCommand.ROLLUP);
                        rollupOpts.setNbBackups(backupOpts.getRollup());

                        operations.addOperation(new RollupBackup(getConf(), backupManifest, rollupOpts));
                        exit = operations.execute();
                        if (exit != 0) return exit;
                    }

                    return 0;
                }

            case RESTORE:
                try (final Connection conn = ConnectionFactory.createConnection(getConf())) {
                    RestoreBackupOptions restoreOpts = parseArgs(new RestoreBackupOptions(), args);
                    backupManifestPath = FileUtils.path(restoreOpts.getBackupRooPath(), BackupManifest.BACKUP_MANIFEST_NAME);


                    Map<String, String> tables;
                    boolean clone = false;
                    if (!Optional.ofNullable(restoreOpts.getTableMapping()).isPresent()) {


                        backupManifest = BackupManifests.readFrom(getConf(), backupManifestPath);
                        tables = backupManifest
                                .getManifests()
                                .stream()
                                .filter(m -> m.getBackupId().equals(restoreOpts.getBackupId()))
                                .flatMap(m -> m.getTables().stream())
                                .collect(Collectors.toMap(x -> x, x -> x));

                        log.info("Restoring tables: {}", tables);

                    } else {
                        clone = true;
                        tables = restoreOpts.getTableMapping();
                        log.info("Restoring to target tables: {}", tables);
                    }


                    for (Map.Entry<String, String> table : tables.entrySet()) {
                        HbaseBackupRestoreOperations operations = new HbaseBackupRestoreOperations();
                        log.info("Restoring table {} to {}", table.getKey(), table.getValue());
                        operations
                                .addOperation(new RestoreBackup(conn, getConf(), table.getKey(), restoreOpts.getBackupId(), restoreOpts))
                                .addOperation(new DisableTable(conn, table.getValue()))
                                .addOperation(new RestoreSnapshot(conn, table.getKey(), table.getValue(), restoreOpts.getBackupId(), clone))
                                .addOperation(new EnableTable(conn, table.getValue()))
                                .addOperation(new DeleteSnapshot(conn, table.getKey(), restoreOpts.getBackupId()));
                        exit = operations.execute();
                        if (exit != 0) return exit;
                    }

                    return 0;
                }


            case HISTORY:
                HistoryBackupOptions historyOpts = parseArgs(new HistoryBackupOptions(), args);
                return new HbaseBackupRestoreOperations()
                        .addOperation(new HistoryBackup(getConf(), historyOpts))
                        .execute();

            case ROLLUP:
                HbaseBackupRestoreOperations rollup = new HbaseBackupRestoreOperations();
                RollupBackupOptions rollupBackupOpts = parseArgs(new RollupBackupOptions(), args);
                backupManifestPath = FileUtils.path(rollupBackupOpts.getBackupRooPath(), BackupManifest.BACKUP_MANIFEST_NAME);
                backupManifest = BackupManifests
                        .readFrom(getConf(), backupManifestPath);
                return new HbaseBackupRestoreOperations()
                        .addOperation(new RollupBackup(getConf(), backupManifest, rollupBackupOpts))
                        .execute();

            default:
                return new HbaseBackupRestoreOperations()
                        .addOperation(new HelpBackup(getConf()))
                        .execute();

        }


    }


    private <T> T parseArgs(T options, String[] args) {
        CommandLine cli = new CommandLine(options)
                .setCaseInsensitiveEnumValuesAllowed(true);

        boolean printUsage = false;

        try {
            cli.parse(args);
        } catch (Exception e) {
            printUsage = true;
        }

        if (printUsage || cli.isUsageHelpRequested()) {
            cli.usage(System.out);
            System.exit(0);
        }

        return options;
    }

}
