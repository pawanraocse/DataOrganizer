package com.tech;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;

public class CopyFileTask implements Runnable {

    static Logger logger = LogManager.getLogger(CopyFileTask.class);
    private final File fromPath;
    private final File toPath;
    private static final int DEFAULT_BLOCK_SIZE = 4096;
    private final int copyBlockSize;
    private final boolean useStreamCopy;

    public CopyFileTask(File fromPath, File toPath) {
        this(fromPath, toPath, DEFAULT_BLOCK_SIZE, true);
    }

    public CopyFileTask(File fromPath, File toPath, int copyBlockSize, boolean useStreamCopy) {
        this.fromPath = fromPath;
        this.toPath = toPath;
        this.copyBlockSize = copyBlockSize;
        this.useStreamCopy = useStreamCopy;
    }

    @Override
    public void run() {
        logger.info("Starting copy file {} to {}", fromPath, toPath);
        try {
            if (useStreamCopy) {
                copyUsingChunks();
            } else {
                copyUsingJava();
            }
            logger.info("Completed copy file {} to {}", fromPath, toPath);
        } catch (IOException e) {
            logger.error(new ParameterizedMessage("Failed to copy file {0} to the destination {1}", fromPath, toPath));
            throw new RuntimeException(e);
        }
    }

    private void copyUsingJava() throws IOException {
        Files.copy(fromPath.toPath(), toPath.toPath(), StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.COPY_ATTRIBUTES);
    }

    private void copyUsingChunks() throws IOException {
        try (
            InputStream inputStream = new FileInputStream(fromPath);
            BufferedInputStream bufferedInputStream = new BufferedInputStream(inputStream);
            OutputStream outputStream = new FileOutputStream(toPath);
        ) {
            byte[] buffer = new byte[copyBlockSize];
            int read;
            while ((read = bufferedInputStream.read(buffer, 0, buffer.length)) != -1) {
                outputStream.write(buffer, 0, read);
            }
        }
    }
}
