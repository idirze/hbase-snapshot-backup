package com.idirze.hbase.snapshot.backup.cli;


import lombok.Data;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(name = "restore",
        mixinStandardHelpOptions = true,
        description = "Restore an image from a backup",
        showDefaultValues = true)
@Data
public class RestoreBackupOptions extends BackupOptions {

    @Option(names = "-backup_id",
            description = "The id identifying the backup image",
            required = true)
    private String backupId;
}
