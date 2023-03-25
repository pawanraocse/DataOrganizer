package com.tech.utils;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.zip.CRC32;
import java.util.zip.Checksum;

public class CheckSumUtil {
    static Logger logger = LogManager.getLogger(CheckSumUtil.class);

    public static final int DEFAULT_BLOCK_SIZE = 4096;
    public static final String DEFAULT_SCHEME = "SHA-256";

    private static class InstanceHolder {

        private static final CheckSumUtil INSTANCE = new CheckSumUtil();
    }

    public static CheckSumUtil getInstance() {
        return InstanceHolder.INSTANCE;
    }

    public static MessageDigest getDefaultMessageDigest() throws NoSuchAlgorithmException {
        return MessageDigest.getInstance(DEFAULT_SCHEME);
    }

    public String getFileChecksum(File file) throws IOException, NoSuchAlgorithmException {
        return getFileChecksum(getDefaultMessageDigest(), file);
    }

    public String getFileChecksum(MessageDigest digest, File file) throws IOException {
        return getFileChecksum(digest, file, DEFAULT_BLOCK_SIZE);
    }

    public String getFileChecksum(MessageDigest digest, File file, int blockSize) throws IOException {
        logger.info("Calculating checksum for the file {}, block size {} and digest algo {}", file.getPath(), blockSize, digest.getAlgorithm());

        if (blockSize <= 0) {
            blockSize = DEFAULT_BLOCK_SIZE;
        }

        try (FileInputStream fis = new FileInputStream(file)) {
            byte[] byteArray = new byte[blockSize];
            int bytesCount;
            while ((bytesCount = fis.read(byteArray)) != -1) {
                digest.update(byteArray, 0, bytesCount);
            }
        }

        byte[] bytes = digest.digest();
        StringBuilder sb = new StringBuilder();
        for (final byte aByte : bytes) {
            sb.append(Integer.toString((aByte & 0xff) + 0x100, 16).substring(1));
        }
        logger.info("Successfully Calculated checksum for the file {}", file.getPath());

        return sb.toString();
    }

    public boolean isValidCheckSumScheme(String scheme) {
        if (scheme == null) {
            return false;
        }
        scheme = scheme.toUpperCase();
        return scheme.equals("SHA-1") || scheme.equals("SHA-256") || scheme.equals("MD5") || scheme.equals("CRC32");
    }

    public String getFileChecksum(File file, String checkSumScheme) throws IOException, NoSuchAlgorithmException {
        return getFileChecksum(file, checkSumScheme, DEFAULT_BLOCK_SIZE);
    }

    public String getFileChecksum(File file, String checkSumScheme, int blockSize) throws IOException, NoSuchAlgorithmException {
        if (isValidCheckSumScheme(checkSumScheme)) {
            checkSumScheme = DEFAULT_SCHEME;
        }
        checkSumScheme = checkSumScheme.toUpperCase();

        if (checkSumScheme.equals("CRC32")) {
            return String.valueOf(calculateChecksumUsingCRC32(file));
        } else {
            return getFileChecksum(MessageDigest.getInstance(checkSumScheme), file, blockSize);
        }
    }

    public long calculateChecksumUsingCRC32(File file) throws IOException {
        return calculateChecksumUsingCRC32(file, DEFAULT_BLOCK_SIZE);
    }

    public long calculateChecksumUsingCRC32(File file, int blockSize) throws IOException {
        logger.info("Calculating crc32 checksum for the file {}", file.getPath());
        try (FileInputStream stream = new FileInputStream(file)) {
            Checksum sum = new CRC32();
            byte[] buf = new byte[blockSize];
            int count;
            while ((count = stream.read(buf)) != -1) {
                if (count > 0) {
                    sum.update(buf, 0, count);
                }
            }
            logger.info("Successfully calculated crc32 checksum for the file {}", file.getPath());
            return sum.getValue();
        }
    }

}
