package com.idirze.hbase.snapshot.backup.cli;


import com.idirze.hbase.snapshot.backup.commad.BackupCommand;
import lombok.Data;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

@Command(name = "rollup",
        mixinStandardHelpOptions = true,
        description = "Roll out a backup",
        showDefaultValues = true)
@Data
public class RollupBackupOptions {

    @Parameters( index = "0",
            hidden = true)
    private BackupCommand command;

    @Option(names = "-nb_backups",
            description = "The number of incremental backups to maintain",
            required = true)
    private Integer nbBackups;

    @Option(names = "-backup_root_path",
            description = "The full root path to store the backup image: hdfs|file:///path/to/backup/dir",
            required = true)
    private String backupRooPath;

    @Option(names = "-roll_up_dir",
            description = "The directory where to save the rolled out files",
            required = false)
    private String rollupDir;
}
