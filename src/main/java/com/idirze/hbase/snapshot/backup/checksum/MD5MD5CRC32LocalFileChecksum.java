package com.idirze.hbase.snapshot.backup.checksum;

import org.apache.hadoop.fs.FileChecksum;
import org.apache.hadoop.io.MD5Hash;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class MD5MD5CRC32LocalFileChecksum extends FileChecksum {


    private int bytesPerCRC;
    private long crcPerBlock;
    private MD5Hash md5;

    @Override
    public String getAlgorithmName() {
        return null;
    }

    @Override
    public int getLength() {
        return 0;
    }

    @Override
    public byte[] getBytes() {
        return new byte[0];
    }

    @Override
    public void write(DataOutput out) throws IOException {

    }

    @Override
    public void readFields(DataInput in) throws IOException {

    }
}
