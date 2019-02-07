package com.idirze.hbase.snapshot.backup.utils;

import static com.google.common.base.Preconditions.checkNotNull;

public class FileUtils {


    public static String path(String dir, String fileName) {
        checkNotNull(dir, "The directory name should not be null");
        return dir.endsWith("/") ? dir + fileName : dir + "/" + fileName;
    }


}
