package com.tech.utils;

import org.apache.commons.io.FilenameUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;

public class FileUtil {
    static Logger logger = LogManager.getLogger(FileUtil.class);
    private FileUtil() {
    }
    public static String getExtensionByApacheCommonLib(String filename) {
        return FilenameUtils.getExtension(filename);
    }

    public static boolean appendEntryToLogFile(File logFilePath, String contentToAppend, boolean throwException) {
        if (!logFilePath.getParentFile().exists()) {
            logFilePath.getParentFile().mkdirs();
        }
        try {
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
}
