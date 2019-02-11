package com.idirze.hbase.snapshot.backup;

import com.idirze.hbase.snapshot.backup.commad.BackupRestoreCommand;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;

@Slf4j
public class HbaseBackupRestorePipeline {

    private List<BackupRestoreCommand> commands = new ArrayList<>();

    public int execute() {
        for (int i = 0; i < commands.size(); i++) {
            BackupRestoreCommand cmd = commands.get(i);
            try {
                cmd.execute();
            } catch (Exception e) {
                try {
                    log.error("Failed to create backup, rollbacking", e);
                    for (int j = i; j >= 0; j--) {
                        BackupRestoreCommand rollbackCmd = commands.get(j);
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

    public HbaseBackupRestorePipeline addStage(BackupRestoreCommand command) {
        this.commands.add(command);
        return this;
    }
}
