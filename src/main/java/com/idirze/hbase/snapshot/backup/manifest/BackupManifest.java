package com.idirze.hbase.snapshot.backup.manifest;

import com.idirze.hbase.snapshot.backup.commad.BackupCommand;
import com.idirze.hbase.snapshot.backup.commad.BackupStatus;
import lombok.Getter;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

@Getter
public class BackupManifest {

    public static final String BACKUP_MANIFEST_NAME = "its_backup.manifest";
    public static DateTimeFormatter pattern = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss.SSZZ");

    private String backupId;
    private String date;
    private BackupCommand command;
    private String backupRootDir;
    private Set<String> tables = new HashSet<>();
    private BackupStatus status;

    public BackupManifest withBackupId(String backupId) {
        this.backupId = backupId;
        return this;
    }


    public BackupManifest withDate(DateTime date) {
        this.date = date.toString(pattern);
        return this;
    }

    public BackupManifest withCommand(BackupCommand command) {
        this.command = command;
        return this;
    }

    public BackupManifest withBackupRootDir(String backupRootDir) {
        this.backupRootDir = backupRootDir;
        return this;
    }


    public BackupManifest withBackupStatus(BackupStatus status) {
        this.status = status;
        return this;
    }

    public BackupManifest addTable(String table) {
        this.tables.add(table);
        return this;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        BackupManifest that = (BackupManifest) o;
        return Objects.equals(backupId, that.backupId) &&
                Objects.equals(tables, that.tables);
    }

    @Override
    public int hashCode() {
        return Objects.hash(backupId, tables);
    }

    @Override
    public String toString() {
        return "backupId: " + backupId + "\n" +
                "date: " + date + "\n" +
                "command: " + command + "\n" +
                "backupRootDir: " + backupRootDir + "\n" +
                "tables: " + tables+ "\n" +
                "status: " + status;
    }

}
