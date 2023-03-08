package com.tech.utils;

import org.apache.commons.io.FilenameUtils;

public class FileUtil {

    private FileUtil(){}
    public static String getExtensionByApacheCommonLib(String filename) {
        return FilenameUtils.getExtension(filename);
    }
}
