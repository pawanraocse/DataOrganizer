package com.tech;

import com.tech.utils.FileUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.BufferedInputStream;
import java.io.File;
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
    private final boolean failFast;

    public CopyFileTask(File fromPath, File toPath) {
        this(fromPath, toPath, DEFAULT_BLOCK_SIZE, true, true);
    }

    public CopyFileTask(File fromPath, File toPath, int copyBlockSize, boolean useStreamCopy, boolean failFast) {
        this.fromPath = fromPath;
        this.toPath = toPath;
        this.copyBlockSize = copyBlockSize;
        this.useStreamCopy = useStreamCopy;
        this.failFast = failFast;
    }

    @Override
    public void run() {
        logger.info("Starting copy file {} to {}", fromPath, toPath);
        String contentToAppend = fromPath.getPath() + "->" + toPath.getPath() + "\n";
        try {
            if (useStreamCopy) {
                copyUsingChunks();
            } else {
                copyUsingJava();
            }
            logger.info("Completed file copy from {} to {}", fromPath, toPath);
            FileUtil.appendEntryToLogFile(DataOrganizerApplication.getCopiedFileLogPath(), contentToAppend, failFast);
        } catch (Exception e) {
            logger.error(e);
            logger.error("Failed to copy file {} to the destination {}", fromPath, toPath);
            FileUtil.appendEntryToLogFile(DataOrganizerApplication.getFailedFileLogPath(), contentToAppend, failFast);
            if (failFast) {
                throw new RuntimeException(e);
            }
        }
    }

    private void copyUsingJava() throws IOException {
        Files.copy(fromPath.toPath(), toPath.toPath(), StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.COPY_ATTRIBUTES);
    }

    private void copyUsingChunks() throws IOException {
        try (
            InputStream inputStream = Files.newInputStream(fromPath.toPath());
            BufferedInputStream bufferedInputStream = new BufferedInputStream(inputStream);
            OutputStream outputStream = Files.newOutputStream(toPath.toPath());
        ) {
            byte[] buffer = new byte[copyBlockSize];
            int read;
            while ((read = bufferedInputStream.read(buffer, 0, buffer.length)) != -1) {
                outputStream.write(buffer, 0, read);
            }
        }
    }
}
