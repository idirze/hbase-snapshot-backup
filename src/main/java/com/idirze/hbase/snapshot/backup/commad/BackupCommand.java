package com.idirze.hbase.snapshot.backup.commad;

import com.google.common.base.Strings;

public enum BackupCommand {

    CREATE,
    RESTORE,
    DELETE,
    HISTORY,
    ROLLOUT,
    HELP;

    public static BackupCommand backupCommandOf(String cmd) {
        try {
            return BackupCommand.valueOf(Strings.nullToEmpty(cmd).toUpperCase());
        } catch (IllegalArgumentException e) {
            return HELP;
        }

    }
}
