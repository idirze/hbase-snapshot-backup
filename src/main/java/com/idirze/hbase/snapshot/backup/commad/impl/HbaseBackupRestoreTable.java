package com.idirze.hbase.snapshot.backup.commad.impl;

import com.idirze.hbase.snapshot.backup.commad.BackupRestoreOperation;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Slf4j
public class HbaseBackupRestoreTable {

    private List<BackupRestoreOperation> commands = new ArrayList<>();

    public int execute() {

        for (int i = 0; i < commands.size(); i++) {
            BackupRestoreOperation cmd = commands.get(i);
            try {
                cmd.execute();
            } catch (Exception e) {
                try {
                    log.error("Failed to create backup, rollbacking", e);
                    for (int j = i; j >= 0; j--) {
                        BackupRestoreOperation rollbackCmd = commands.get(j);
                        rollbackCmd.rollback();
                    }
                } catch (Exception e1) {
                    log.error("Failed to rollback ");
                }
                return -1;
            }
        }
        return 0;
    }

    public HbaseBackupRestoreTable addOperation(BackupRestoreOperation command) {
        this.commands.add(command);
        return this;
    }


}
