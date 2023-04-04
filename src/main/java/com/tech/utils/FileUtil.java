package com.tech.utils;

import com.tech.DataOrganizerApplication;
import org.apache.commons.io.FilenameUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

public class FileUtil {

    static Logger logger = LogManager.getLogger(FileUtil.class);

    private FileUtil() {
    }

    public static String getExtensionByApacheCommonLib(String filename) {
        return FilenameUtils.getExtension(filename);
    }

    public static boolean appendEntryToLogFile(File logFilePath, String contentToAppend, boolean throwException) {
        try {
            if (!logFilePath.getParentFile().exists() && (!logFilePath.getParentFile().mkdirs())) {
                logger.error("Failed to create directory {}", logFilePath.getParentFile().getPath());
            }
            Files.write(
                logFilePath.toPath(),
                contentToAppend.getBytes(),
                logFilePath.exists() ? StandardOpenOption.APPEND : StandardOpenOption.CREATE);
        } catch (IOException e) {
            logger.error(e);
            if (throwException) {
                throw new RuntimeException(e);
            }
        }
        return true;
    }

    public static void main(String[] args) {
        final File failedFileLogPath = DataOrganizerApplication.getFailedFileLogPath();
        final File file = appendSuffix(failedFileLogPath, "-1");
        System.out.println(file.getPath());
    }

    public static File appendSuffix(File file, String suffix) {
        String path = file.getPath();
        final int indexOfDot = path.lastIndexOf(".");
        if (indexOfDot > -1) {
            file = new File(path.substring(0, indexOfDot) + suffix + path.substring(indexOfDot));
        } else {
            file = new File(path + suffix);
        }
        return file;
    }

    public static void rename(final File file, final String newName) {
        Path filePath = file.toPath();
        try {
            Files.move(filePath, filePath.resolveSibling(newName));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
