package com.idirze.hbase.snapshot.backup.commad;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class BackupId {

    private String id;
    private boolean isNew;

}
