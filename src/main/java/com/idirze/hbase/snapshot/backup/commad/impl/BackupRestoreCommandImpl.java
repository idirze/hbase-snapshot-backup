package com.idirze.hbase.snapshot.backup.commad.impl;

import com.idirze.hbase.snapshot.backup.HbaseBackupRestorePipeline;
import com.idirze.hbase.snapshot.backup.cli.*;
import com.idirze.hbase.snapshot.backup.commad.BackupCommand;
import com.idirze.hbase.snapshot.backup.hbase.*;
import com.idirze.hbase.snapshot.backup.manifest.BackupManifest;
import com.idirze.hbase.snapshot.backup.manifest.BackupManifests;
import com.idirze.hbase.snapshot.backup.utils.FileUtils;
import com.idirze.hbase.snapshot.backup.utils.SnapshotBackupUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.util.Tool;
import picocli.CommandLine;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

@Slf4j
public class BackupRestoreCommandImpl extends Configured implements Tool {

    public static final String BACKUPID_PREFIX = "its_backup_";

    public BackupRestoreCommandImpl(Configuration conf) {
        setConf(conf);
    }

    @Override
    public int run(String[] args) throws Exception {
        String command = args.length == 0 ? "-h" : args[0];
        HbaseBackupRestorePipeline pipeline = new HbaseBackupRestorePipeline();
        String backupManifestPath;
        BackupManifests backupManifest;

        switch (BackupCommand.backupCommandOf(command)) {
            case CREATE:

                CreateBackupOptions backupOpts = parseArgs(new CreateBackupOptions(), args);
                String backupId = BACKUPID_PREFIX + EnvironmentEdgeManager.currentTime();

                backupManifestPath = FileUtils.path(backupOpts.getBackupRooPath(), BackupManifest.BACKUP_MANIFEST_NAME);

                backupManifest = BackupManifests.readFrom(getConf(), backupManifestPath);

                List<TableName> tableNames = null;
                if (backupOpts.getNamespaces() != null && !backupOpts.getNamespaces().isEmpty()) {
                    log.info("Performing backup for namespaces: {}", backupOpts.getNamespaces());
                    tableNames = SnapshotBackupUtils.listTableNamesByNamespaces(getConf(), backupOpts.getNamespaces());
                } else if (backupOpts.getTables() != null && !backupOpts.getTables().isEmpty()) {
                    log.info("Performing backup for tables: {}", backupOpts.getNamespaces());
                    tableNames = SnapshotBackupUtils.listTableNames(getConf(), backupOpts.getTables());
                } else {
                    log.error("-namespaces or -table should be provided");
                    System.exit(-1);
                }


                log.info("Found tables: {}", tableNames.stream().map(t -> t.getNameAsString()).collect(Collectors.toList()));

                for (TableName tableName : tableNames) {
                    pipeline.addStage(new CreateSnapshot(getConf(), tableName.getNameAsString(), backupId))
                            .addStage(new CreateBackup(getConf(), tableName.getNameAsString(), backupId, backupManifest, backupOpts))
                            .addStage(new DeleteSnapshot(getConf(), tableName.getNameAsString(), backupId));
                }

                if (backupOpts.getRollout() != -1) {
                    RolloutBackupOptions rolloutOpts = new RolloutBackupOptions();
                    rolloutOpts.setBackupRooPath(backupOpts.getBackupRooPath());
                    rolloutOpts.setCommand(BackupCommand.ROLLOUT);
                    rolloutOpts.setNbBackups(backupOpts.getRollout());

                    pipeline.addStage(new RollOutBackup(getConf(), backupManifest, rolloutOpts));
                }

                break;

            case RESTORE:

                RestoreBackupOptions restoreOpts = parseArgs(new RestoreBackupOptions(), args);
                backupManifestPath = FileUtils.path(restoreOpts.getBackupRooPath(), BackupManifest.BACKUP_MANIFEST_NAME);

                backupManifest = BackupManifests.readFrom(getConf(), backupManifestPath);
                Set<String> tables = backupManifest
                        .getManifests()
                        .stream()
                        .filter(m -> m.getBackupId().equals(restoreOpts.getBackupId()))
                        .flatMap(m -> m.getTables().stream())
                        .collect(Collectors.toSet());

                for (String table : tables) {

                    pipeline
                            .addStage(new RestoreBackup(getConf(), table, restoreOpts.getBackupId(), restoreOpts))
                            .addStage(new DisableTable(getConf(), table))
                            .addStage(new RestoreSnapshot(getConf(), table, restoreOpts.getBackupId()))
                            .addStage(new EnableTable(getConf(), table))
                            .addStage(new DeleteSnapshot(getConf(), table, restoreOpts.getBackupId()));
                }

                break;

            case DELETE:
                BackupOptions deleteOpts = parseArgs(new BackupOptions(), args);
                pipeline.addStage(new DeleteBackup(getConf(), deleteOpts));
                break;

            case HISTORY:
                HistoryBackupOptions historyOpts = parseArgs(new HistoryBackupOptions(), args);
                pipeline.addStage(new HistoryBackup(getConf(), historyOpts));
                break;

            case ROLLOUT:
                RolloutBackupOptions rolloutBackupOpts = parseArgs(new RolloutBackupOptions(), args);
                backupManifestPath = FileUtils.path(rolloutBackupOpts.getBackupRooPath(), BackupManifest.BACKUP_MANIFEST_NAME);
                backupManifest = BackupManifests
                        .readFrom(getConf(), backupManifestPath);
                pipeline.addStage(new RollOutBackup(getConf(), backupManifest, rolloutBackupOpts));
                break;

            default:
                pipeline.addStage(new HelpBackup(getConf()));

        }


        return pipeline.execute();
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
