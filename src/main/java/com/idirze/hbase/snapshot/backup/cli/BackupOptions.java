package com.idirze.hbase.snapshot.backup.cli;


import com.idirze.hbase.snapshot.backup.commad.BackupCommand;
import lombok.Data;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

import java.util.List;

import static picocli.CommandLine.Help.Visibility.ALWAYS;

@Command(mixinStandardHelpOptions = true,
        description = "Prints usage help.",
        showDefaultValues = true)
@Data
public class BackupOptions {

    @Parameters(index = "0",
            hidden = true)
    private BackupCommand command;

    @Option(names = "-backup_root_path",
            description = "The full root path to store the backup image: hdfs|file:///path/to/backup/dir",
            required = true)
    private String backupRooPath;

    @Option(names = "-namespaces",
            description = "Namespaces to backup",
            required = false,
            arity = "1..*",
            split = ",")
    private List<String> namespaces;

    @Option(names = "-tables",
            description = "Tables to backup",
            required = false,
            arity = "1..*",
            split = ",")
    private List<String> tables;

    @CommandLine.Option(names = "-skip_tmp",
            description = "Skip HDFS /tmp directory",
            required = false,
            defaultValue = "false",
            showDefaultValue = ALWAYS)
    private boolean skipTmp;

    @Option(names = "-no-checksum-verify",
            description = "Do not verify checksum, use name+length only",
            required = false,
            defaultValue = "false",
            showDefaultValue = ALWAYS)
    private boolean noChecksumVerify;

    @Option(names = "-mappers",
            description = "Number of mappers to use during the copy (mapreduce.job.maps).",
            required = false,
            defaultValue = "0")
    private Integer mappers;

    @Option(names = "-bandwidth",
            description = "Limit bandwidth to this value in MB/second.",
            required = false,
            defaultValue = "" + Integer.MAX_VALUE)
    private Integer bandwidth;

    @Option(names = "-chuser",
            description = "Change the owner of the files to the specified one.",
            required = false)
    private String chuser;


    @Option(names = "-chgroup",
            description = "Change the group of the files to the specified one.",
            required = false)
    private String chgroup;

    @Option(names = "-chmod",
            description = "Change the permission of the files to the specified one.",
            required = false,
            defaultValue = "0")
    private Integer chmod;

    private String inputRootPath;


}
