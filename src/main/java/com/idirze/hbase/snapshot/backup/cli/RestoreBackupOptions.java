package com.idirze.hbase.snapshot.backup.cli;


import lombok.Data;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

import java.util.List;
import java.util.Map;
import java.util.Set;

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


    @Option(names = "-restore_root_path",
            description = "The full root path to restore the backup image: hdfs://nameNode:port/apps/hbase/data",
            required = false)
    private String restoreRooPath;

    @Option(names = "-mapping",
            description = "A key=value of source/target tables. If specified, each table in <tables> must have a mapping." +
                    "Ex: -mapping src1=target1 -mapping src2=target2",
            required = false)
    private Map<String, String> tableMapping;
}
