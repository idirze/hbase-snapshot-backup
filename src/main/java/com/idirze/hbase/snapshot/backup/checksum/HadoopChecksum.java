package com.idirze.hbase.snapshot.backup.checksum;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.util.DataChecksum;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.security.DigestInputStream;
import java.security.NoSuchAlgorithmException;

@Slf4j
public class HadoopChecksum {
    /**
     * @Param filePath : Local file path
     * @Param bytesPerChecksum : HDFS bytes per checksum. Default 512.
     * @Param crcsPerBlock : HDFS checksum per block. Default 134217728.
     * @Param checksumType : DataChecksum type. Ideally DataChecksum.Type.CRC32C
     */
    public static String calculate(String filePath, int bytesPerChecksum, int crcsPerBlock, DataChecksum.Type checksumType)
            throws NoSuchAlgorithmException, IOException {
        MD5MD5CRCMessageDigest messageDigest = new MD5MD5CRCMessageDigest(bytesPerChecksum, crcsPerBlock, checksumType);
        FileInputStream fileInputStream = new FileInputStream(filePath);
        DigestInputStream digestInputStream = new DigestInputStream(fileInputStream, messageDigest);
        OutputStream outputStream = new NullOutputStream();
        IOUtils.copy(digestInputStream, outputStream);
        return new String(Hex.encodeHex(messageDigest.digest()));
    }

    public static String calculate(String filePath) {
        try {
            return calculate(filePath, 512, 134217728, DataChecksum.Type.CRC32C);
        } catch (NoSuchAlgorithmException | IOException e) {
            return null;
        }
    }
}