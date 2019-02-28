package com.idirze.hbase.snapshot.backup.utils;

import com.idirze.hbase.snapshot.backup.hbase.HFileLink;
import com.idirze.hbase.snapshot.backup.hbase.WALLink;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos;
import org.apache.hadoop.hbase.protobuf.generated.SnapshotProtos;
import org.apache.hadoop.hbase.protobuf.generated.SnapshotProtos.SnapshotFileInfo.Type;
import org.apache.hadoop.hbase.snapshot.SnapshotDescriptionUtils;
import org.apache.hadoop.hbase.snapshot.SnapshotReferenceUtil;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.Triple;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

@Slf4j
public class SnapshotBackupUtils {

    private final static long TABLE_AVAILABILITY_WAIT_TIME = 180000;

    public static String tableSnapshotId(String backupId, String tableName) {

        return new StringBuilder()
                .append(backupId)
                .append("_")
                .append(tableName.replace(":", "_"))
                .toString();
    }

    /**
     * Extract the list of files (HFiles/WALs) to copy using Map-Reduce.
     *
     * @return list of files referenced by the snapshot (pair of path and size)
     */
    public static List<Triple<String, Long, Type>> getSnapshotFiles(final Configuration conf,
                                                                    final FileSystem fs, final Path snapshotDir) throws IOException {
        HBaseProtos.SnapshotDescription snapshotDesc = SnapshotDescriptionUtils.readSnapshotInfo(fs, snapshotDir);

        final List<Triple<String, Long, Type>> files = new ArrayList();
        final TableName table = TableName.valueOf(snapshotDesc.getTable());

        SnapshotReferenceUtil.visitReferencedFiles(conf, fs, snapshotDir, snapshotDesc,
                new SnapshotReferenceUtil.SnapshotVisitor() {
                    @Override
                    public void storeFile(final HRegionInfo regionInfo, final String family,
                                          final SnapshotProtos.SnapshotRegionManifest.StoreFile storeFile) throws IOException {
                        if (storeFile.hasReference()) {
                            // copied as part of the manifest
                        } else {
                            String region = regionInfo.getEncodedName();
                            String hfile = storeFile.getName();
                            String hfilePath1 = createHFilePath(table, region, family, hfile);
                            String hfilePath2 = hfilePath1.replace(new StringBuilder(table.getNameAsString().replace(":", "=")).append("=").append(region).append("-").toString(), "");

                            long size;
                            if (storeFile.hasFileSize()) {
                                size = storeFile.getFileSize();
                            } else {
                                size = HFileLink.buildFromHFileLinkPattern(conf, HFileLink.createPath(table, region, family, hfile))
                                        .getFileStatus(fs)
                                        .getLen();
                            }
                            files.add(new Triple(hfilePath1, size, Type.HFILE));
                            files.add(new Triple(hfilePath2, size, Type.HFILE));
                        }
                    }

                    @Override
                    public void logFile(final String server, final String logfile)
                            throws IOException {
                        WALLink walLink = new WALLink(conf, server, logfile);
                        files.add(new Triple(walLink.getFileStatus(fs).getPath(), walLink.getFileStatus(fs).getLen(), Type.WAL));
                    }
                });

        return files;
    }


    public static String createHFilePath(final TableName table, final String region,
                                         final String family, final String hfile) {
        return String.format("%s/%s/%s/%s",
                table.getNameAsString().replace(TableName.NAMESPACE_DELIM, '/'),
                region, family, hfile);
    }


    public static void createSnapshot(Connection connection,
                                      String tableName,
                                      String snapshotName,
                                      String snapshotType) throws Exception {

        try (Admin admin = connection.getAdmin()) {
            HBaseProtos.SnapshotDescription.Type type = HBaseProtos.SnapshotDescription.Type.FLUSH;
            if (snapshotType != null) {
                type = HBaseProtos.SnapshotDescription.Type.valueOf(snapshotType.toUpperCase());
            }
            admin.snapshot(snapshotName, TableName.valueOf(tableName), type);
        }
    }

    public static void deleteSnapshot(Connection connection,
                                      String snapshotName) throws Exception {

        try (Admin admin = connection.getAdmin()) {
            admin.deleteSnapshot(snapshotName);
        }
    }

    public static void restoreSnapshot(Connection connection,
                                       String snapshotName) throws Exception {
        try (Admin admin = connection.getAdmin()) {
            admin.restoreSnapshot(snapshotName);
        }
    }

    public static void cloneSnapshot(Connection connection,
                                     String snapshotName, String table) throws Exception {
        try (Admin admin = connection.getAdmin()) {
            TableName tableName = TableName.valueOf(table);
            if (admin.tableExists(tableName)) {
                if (admin.isTableEnabled(tableName)) {
                    admin.disableTable(tableName);
                }
                log.info("Dropping the target table {}", table);
                admin.deleteTable(tableName);
            }

            log.info("Cloning the snapshot {} into the target table {}", snapshotName, table);
            admin.cloneSnapshot(snapshotName, tableName);
        }
    }

    public static void enableTable(Connection connection,
                                   String table) throws Exception {
        try (Admin admin = connection.getAdmin()) {
            TableName tableName = TableName.valueOf(table);
            if (admin.tableExists(tableName)
                    && admin.isTableDisabled(tableName)) {
                admin.enableTable(tableName);
                checkAvailable(connection, table);
            }
        }
    }

    public static void disableTable(Connection connection,
                                    String table) throws Exception {
        try (Admin admin = connection.getAdmin()) {
            TableName tableName = TableName.valueOf(table);
            if (admin.tableExists(tableName)
                    && admin.isTableEnabled(tableName)) {
                admin.disableTable(tableName);
            }
        }
    }

    public static List<TableName> listTableNamesByNamespaces(Connection connection,
                                                             List<String> namespaces) throws Exception {
        List<TableName> tableNames = new ArrayList<>();
        try (Admin admin = connection.getAdmin()) {

            for (String ns : namespaces) {
                tableNames.addAll(Arrays.asList(admin.listTableNamesByNamespace(ns)));
            }

            return tableNames;

        }
    }

    public static List<TableName> listTableNames(Connection connection,
                                                 List<String> tables) throws Exception {
        List<TableName> tableNames = new ArrayList<>();
        try (Admin admin = connection.getAdmin()) {

            tableNames.addAll(Arrays.asList(admin.listTableNames())
                    .stream()
                    .filter(t -> tables.contains(t.getNameAsString()))
                    .collect(Collectors.toList()));

            return tableNames;

        }
    }


    public static boolean checkAvailable(Connection connection,
                                         String table) throws Exception {
        try (Admin admin = connection.getAdmin()) {

            long startTime = EnvironmentEdgeManager.currentTime();
            while (!admin.isTableAvailable(TableName.valueOf(table))) {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                }
                if (EnvironmentEdgeManager.currentTime() - startTime > TABLE_AVAILABILITY_WAIT_TIME) {
                    throw new IOException("Time out " + TABLE_AVAILABILITY_WAIT_TIME + "ms expired, table "
                            + table + " is still not available");
                }
            }
        }

        return true;

    }


    public static void createNamespaceIfNotExistsForTable(Connection connection, final String table) throws IOException {

        try (Admin admin = connection.getAdmin()) {

            String namespaceName = TableName.valueOf(table).getNamespaceAsString();
            NamespaceDescriptor ns = NamespaceDescriptor.create(namespaceName).build();
            NamespaceDescriptor[] list = admin.listNamespaceDescriptors();
            boolean exists = false;
            for (NamespaceDescriptor nsd : list) {
                if (nsd.getName().equals(ns.getName())) {
                    exists = true;
                    break;
                }
            }
            if (!exists) {
                log.info("Creating namespace: " + ns);
                admin.createNamespace(ns);
            }
        }
    }
}
