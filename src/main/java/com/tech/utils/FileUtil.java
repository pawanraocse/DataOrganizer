package com.tech.utils;

import org.apache.commons.io.FilenameUtils;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;

public class FileUtil {

    private FileUtil() {
    }

    public static String getExtensionByApacheCommonLib(String filename) {
        return FilenameUtils.getExtension(filename);
    }

    public static boolean appendEntryToLogFile(File logFilePath, String contentToAppend) {
        try {
            if (!logFilePath.getParentFile().exists()) {
                logFilePath.getParentFile().mkdirs();
            }
            Files.write(
                logFilePath.toPath(),
                contentToAppend.getBytes(),
                logFilePath.exists() ? StandardOpenOption.APPEND : StandardOpenOption.CREATE);
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
        return true;
    }
}
