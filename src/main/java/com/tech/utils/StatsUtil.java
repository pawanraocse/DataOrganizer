package com.tech.utils;

import com.tech.DataOrganizerApplication;
import com.tech.PropFileHandler;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

public class StatsUtil {

    static Logger logger = LogManager.getLogger(StatsUtil.class);

    private AtomicInteger totalFilesCopied;
    private AtomicInteger totalFilesFailed;
    private AtomicInteger totalFilesSkipped;
    private AtomicInteger totalFoldersSkipped;
    private AtomicInteger totalDuplicateFiles;
    private volatile long totalLengthCopied;

    private static volatile StatsUtil instance;
    private static final Object mutex = new Object();

    public static StatsUtil getInstance() {
        if (instance == null) {
            synchronized (mutex) {
                if (instance == null) {
                    instance = new StatsUtil();
                }
            }
        }
        return instance;
    }

    private StatsUtil() {
        readStatsFile();
    }

    private Properties statsPropFile = null;

    private void readStatsFile() {
        try {
            statsPropFile = PropFileHandler.readPropertiesFile(DataOrganizerApplication.getStatsFilePath().getPath());
            totalFilesCopied = new AtomicInteger(PropFileHandler.getInteger(StatsKey.COPIED_FILES.name(), statsPropFile, 0));
            totalFilesFailed = new AtomicInteger(PropFileHandler.getInteger(StatsKey.FAILED_FILES.name(), statsPropFile, 0));
            totalFilesSkipped = new AtomicInteger(PropFileHandler.getInteger(StatsKey.SKIPPED_FILES.name(), statsPropFile, 0));
            totalDuplicateFiles = new AtomicInteger(PropFileHandler.getInteger(StatsKey.DUPLICATE_FILES.name(), statsPropFile, 0));
            totalFoldersSkipped = new AtomicInteger(PropFileHandler.getInteger(StatsKey.SKIPPED_FOLDERS.name(), statsPropFile, 0));
            totalLengthCopied = parseSize(PropFileHandler.getString(StatsKey.TOTAL_COPIED_LENGTH.name(), statsPropFile, "0"));
        } catch (IOException e) {
            logger.error(e);
        }
    }

    public synchronized void updateStats(long length, boolean isCopied, boolean isSkipped, boolean isFailed) {
        if (isCopied) {
            totalFilesCopied.incrementAndGet();
            totalLengthCopied += length;
        } else if (isSkipped) {
            totalFilesSkipped.incrementAndGet();
        } else if (isFailed) {
            totalFilesFailed.incrementAndGet();
        }
    }

    public void updateDupFile() {
        totalDuplicateFiles.incrementAndGet();
    }

    public synchronized void updateFolderStats(boolean isSkipped) {
        if (isSkipped) {
            totalFoldersSkipped.incrementAndGet();
        }
    }

    public synchronized void flushChanges() {
        PropFileHandler.setProperty(StatsKey.COPIED_FILES.name(), totalFilesCopied + "", statsPropFile);
        PropFileHandler.setProperty(StatsKey.FAILED_FILES.name(), totalFilesFailed + "", statsPropFile);
        PropFileHandler.setProperty(StatsKey.SKIPPED_FILES.name(), totalFilesSkipped + "", statsPropFile);
        PropFileHandler.setProperty(StatsKey.SKIPPED_FOLDERS.name(), totalFoldersSkipped + "", statsPropFile);
        PropFileHandler.setProperty(StatsKey.DUPLICATE_FILES.name(), totalDuplicateFiles + "", statsPropFile);

        final String toDisplaySize = readableFileSize(totalLengthCopied);
        PropFileHandler.setProperty(StatsKey.TOTAL_COPIED_LENGTH.name(), toDisplaySize, statsPropFile);
        try {
            PropFileHandler.flush(statsPropFile, DataOrganizerApplication.getStatsFilePath());
        } catch (IOException e) {
            logger.error(e);
        }
    }

    public static String readableFileSize(long size) {
        if (size <= 0) {
            return "0";
        }
        final String[] units = new String[]{"B", "KB", "MB", "GB", "TB"};
        int digitGroups = (int) (Math.log10(size) / Math.log10(1024));
        return new DecimalFormat("#.##").format(size / Math.pow(1024, digitGroups))
            + " " + units[digitGroups];
    }

    private static final long KB_FACTOR = 1024;
    private static final long MB_FACTOR = 1024 * KB_FACTOR;
    private static final long GB_FACTOR = 1024 * MB_FACTOR;
    private static final long TB_FACTOR = 1024 * GB_FACTOR;

    public static long parseSize(String arg0) {
        int spaceNdx = arg0.trim().indexOf(" ");
        if (spaceNdx < 0) {
            return Long.parseLong(arg0);
        }
        double ret = Double.parseDouble(arg0.substring(0, spaceNdx));
        double result = ret;

        switch (arg0.substring(spaceNdx + 1)) {
            case "TB":
            case "TiB":
                result = ret * TB_FACTOR;
                break;
            case "GB":
            case "GiB":
                result = ret * GB_FACTOR;
                break;
            case "MB":
            case "MiB":
                result = ret * MB_FACTOR;
                break;
            case "KB":
            case "KiB":
                result = ret * KB_FACTOR;
                break;

        }
        return (long) result;
    }

    enum StatsKey {
        FAILED_FILES,
        COPIED_FILES,
        SKIPPED_FILES,
        SKIPPED_FOLDERS,
        DUPLICATE_FILES,
        TOTAL_COPIED_LENGTH
    }

}
