package com.tech;

import com.tech.utils.CheckSumUtil;
import com.tech.utils.FileUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellType;
import org.apache.poi.ss.usermodel.DateUtil;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.util.StringUtil;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.security.NoSuchAlgorithmException;
import java.text.DateFormat;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.regex.Pattern;

public class ProcessExecutor {

    static Logger logger = LogManager.getLogger(ProcessExecutor.class);

    private final String targetFolderPath;
    private final String sourceFolderPath;
    private final String inputFile;
    private final String[] pathSequences;

    private final ExecutorService executorService;

    private static final String DEFAULT_FOLDER_SEQUENCE_PATH = "Decade->Series Title->Year->Episode Number";
    private static final String DEFAULT_GUID_NAME = "GUID";
    private static final DecimalFormat decimalFormat = new DecimalFormat("0.#");
    private static final String DEFAULT_DATE_FORMAT = "yyyy-MM-dd";

    private final Properties properties;
    private Set<String> excludeFileTypesSet;
    private List<Pattern> excludePatternList;
    private final String replaceChars;

    private final boolean useStreamCopy;
    private final int blockSize;
    private final String messageDigestAlgo;
    private final boolean failFast;

    public ProcessExecutor(String inputFile, String sourceFolderPath, String targetFolderPath, String folderSequence, Properties properties) {
        logger.info("Initializing executor with received args:\ninputFile {}\nsourceFolderPath {}\ntargetFolderPath {}\nfolder sequence {}",
            inputFile, sourceFolderPath, targetFolderPath, folderSequence);

        this.inputFile = inputFile;
        this.sourceFolderPath = sourceFolderPath;
        this.targetFolderPath = targetFolderPath;
        folderSequence = folderSequence == null || folderSequence.trim().isEmpty() ? DEFAULT_FOLDER_SEQUENCE_PATH : folderSequence;
        pathSequences = folderSequence.split("->");
        this.properties = properties;

        int nThreads = PropFileHandler.getInteger(PropKeysEnum.COPY_THREADS.name(), properties, 3);
        executorService = Executors.newFixedThreadPool(nThreads);

        final String excludeFileTypes = this.properties.getProperty(PropKeysEnum.EXCLUDE_FILE_TYPES.name());
        final String excludePatterns = this.properties.getProperty(PropKeysEnum.EXCLUDE_PATTERNS.name());

        if (StringUtil.isNotBlank(excludeFileTypes)) {
            excludeFileTypesSet = new HashSet<>();
            final String[] excludeFileTypeArray = excludeFileTypes.split("->");
            Arrays.stream(excludeFileTypeArray).forEach(type -> excludeFileTypesSet.add(type.trim().toLowerCase()));
        }

        if (StringUtil.isNotBlank(excludePatterns)) {
            excludePatternList = new ArrayList<>();
            final String[] excludePatternArray = excludePatterns.split("->");
            Arrays.stream(excludePatternArray).forEach(type -> excludePatternList.add(Pattern.compile(type.trim(), Pattern.CASE_INSENSITIVE)));
        }

        replaceChars = this.properties.getProperty(PropKeysEnum.REPLACE_CHARS.name(), null);

        useStreamCopy = PropFileHandler.getBoolean(PropKeysEnum.USE_STREAM_COPY.name(), this.properties, true);
        blockSize = PropFileHandler.getInteger(PropKeysEnum.COPY_BLOCK_SIZE.name(), this.properties, CheckSumUtil.DEFAULT_BLOCK_SIZE);
        messageDigestAlgo = this.properties.getProperty(PropKeysEnum.CHECKSUM_SCHEME.name(), CheckSumUtil.DEFAULT_SCHEME);

        failFast = PropFileHandler.getBoolean(PropKeysEnum.FAIL_FAST.name(), this.properties, true);
    }

    public void readTheExcelInputFile() throws IOException, InterruptedException {
        Workbook workbook;
        Map<String, String> colKeyValueMapInCurrentRow;
        Map<Integer, String> colIndexToHeaderMap = new HashMap<>();
        int colIndex;
        final int start_index = PropFileHandler.getInteger(PropKeysEnum.START_INDEX.name() + "_" + inputFile, this.properties, 0);
        int rowIndex = 0;

        try (FileInputStream file = new FileInputStream(inputFile)) {
            workbook = new XSSFWorkbook(file);
            Sheet sheet = workbook.getSheetAt(0);

            for (Row row : sheet) {
                rowIndex++;
                if (rowIndex != 1 && rowIndex <= start_index) {
                    continue;
                }
                colIndex = 0;
                colKeyValueMapInCurrentRow = new HashMap<>();
                for (Cell cell : row) {
                    CellType cellType = cell.getCellType();
                    colIndex++;

                    if (rowIndex == 1) {
                        colIndexToHeaderMap.put(colIndex, cell.getStringCellValue());
                        continue;
                    }
                    handleColumnValueAndStoreInKeyValueMap(colKeyValueMapInCurrentRow, colIndexToHeaderMap, colIndex, cell, cellType);
                }

                if (rowIndex > 1) {
                    processCopyOperationOnGivenRow(colKeyValueMapInCurrentRow, rowIndex);
                    PropFileHandler.setProperty(PropKeysEnum.START_INDEX.name() + "_" + inputFile, rowIndex + "", this.properties);
                    PropFileHandler.flush(this.properties, DataOrganizerApplication.getPropFilePath());
                }
            }
        }

        logger.info("Completed all tasks, calling final shutdown.");
        executorService.shutdown();

    }

    private static void handleColumnValueAndStoreInKeyValueMap(final Map<String, String> rowKeyValueMap, final Map<Integer, String> colIndexToHeaderMap,
                                                               final int colIndex, final Cell cell,
                                                               final CellType cellType) {
        if (Objects.requireNonNull(cellType) == CellType.NUMERIC && DateUtil.isCellDateFormatted(cell)) {
            breakDateFieldAndStoreInKeyValueMap(rowKeyValueMap, cell);
        } else if (Objects.requireNonNull(cellType) == CellType.NUMERIC) {
            rowKeyValueMap.put(colIndexToHeaderMap.get(colIndex), decimalFormat.format(cell.getNumericCellValue()) + "");
        } else if (cellType == CellType.STRING) {
            rowKeyValueMap.put(colIndexToHeaderMap.get(colIndex), cell.getStringCellValue());
        }
    }

    private static void breakDateFieldAndStoreInKeyValueMap(final Map<String, String> rowKeyValueMap, final Cell cell) {
        DateFormat df = new SimpleDateFormat(DEFAULT_DATE_FORMAT);
        Date date = cell.getDateCellValue();
        final String cellValue = df.format(date);
        final String[] dates = cellValue.split("-");

        //Date should be stored as date, year, decade , month and day
        rowKeyValueMap.put(DateKeys.DECADE.value, dates[0].substring(0, 3) + "0s");
        rowKeyValueMap.put(DateKeys.YEAR.value, dates[0]);
        rowKeyValueMap.put(DateKeys.MONTH.value, dates[1]);
        rowKeyValueMap.put(DateKeys.DAY.value, dates[2]);
    }

    private void processCopyOperationOnGivenRow(final Map<String, String> rowKeyValueMap, final int rowIndex) throws IOException {
        final File targetFolder = createFolderStructureIfNeeded(pathSequences, rowKeyValueMap, targetFolderPath);
        List<Runnable> taskList = new ArrayList<>();
        copyFilesFromSourceToTarget(new File(sourceFolderPath, rowKeyValueMap.get(DEFAULT_GUID_NAME)), targetFolder, taskList);

        logger.info("Awaiting copy operation for row {} to be completed.", rowIndex);
        executeTaskList(taskList);
        logger.info("Completed copy operation for row {}.", rowIndex);

    }

    private File createFolderStructureIfNeeded(String[] pathSequences, Map<String, String> rowEntryKeyValuePair, String outputFolderPath) throws IOException {
        File folderPathToBeCreated = new File(outputFolderPath);
        for (final String pathSequence : pathSequences) {
            String newChildName = rowEntryKeyValuePair.get(pathSequence.trim());
            if (replaceChars != null) {
                newChildName = newChildName.replaceAll(replaceChars, "");
            }
            folderPathToBeCreated = new File(folderPathToBeCreated, newChildName);
        }
        try {
            if (!folderPathToBeCreated.exists() && !folderPathToBeCreated.mkdirs()) {
                logger.error("Failed to create the folder path: {}", folderPathToBeCreated);
            }
        } catch (Exception e) {
            logger.error(e);
        }

        if (!folderPathToBeCreated.exists() && (this.failFast)) {
            throw new IOException("Failed to create the folder path: " + folderPathToBeCreated);
        }

        return folderPathToBeCreated;
    }

    /**
     * @param srcFolder    folder from where files to be copied
     * @param targetFolder target folder where files need to be copied
     * @param taskList     taskList for executing completable future
     * @throws IOException throw exception if any
     */
    private void copyFilesFromSourceToTarget(File srcFolder, File targetFolder, List<Runnable> taskList) throws IOException {
        Files.walkFileTree(srcFolder.toPath(), new SimpleFileVisitor<Path>() {
            @Override
            public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) {
                if (isMatchingExcludePattern(dir.toFile().getPath())) {
                    return FileVisitResult.SKIP_SUBTREE;
                }
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) {
                if (!Files.isDirectory(file)) {
                    if (isMatchingExcludePattern(file.toFile().getPath()) || isMatchingExcludeFileTypes(file.toFile().getPath())) {
                        return FileVisitResult.CONTINUE;
                    }
                    final File targetFile = getTargetFile(file, targetFolder);

                    if (checkIfFileAlreadyCopied(file, targetFile)) {
                        return FileVisitResult.CONTINUE;
                    }
                    addNewCopyTask(file, targetFile, taskList, srcFolder);
                }
                return FileVisitResult.CONTINUE;
            }
        });
    }

    private void addNewCopyTask(final Path file, final File targetFile, final List<Runnable> taskList, final File srcFolder) {
        taskList.add(new CopyFileTask(file.toFile(), targetFile, blockSize, useStreamCopy, failFast));
        if (taskList.size() >= 1000) {
            executeTaskList(taskList);
            logger.info("Completed copy operation on batch on files inside {}", srcFolder.getPath());
        }
    }

    private boolean checkIfFileAlreadyCopied(final Path file, final File targetFile) {
        return targetFile.exists() && (verifyTheFileIsSame(file.toFile(), targetFile));
    }

    private File getTargetFile(final Path file, final File targetFolder) {
        String targetFileName = file.toFile().getName();
        if (replaceChars != null) {
            targetFileName = targetFileName.replaceAll(replaceChars, "");
        }
        return new File(targetFolder, targetFileName);
    }

    private void executeTaskList(final List<Runnable> taskList) {
        CompletableFuture<?>[] futures = taskList.stream()
            .map(task -> CompletableFuture.runAsync(task, executorService))
            .toArray(CompletableFuture[]::new);
        logger.info("Starting copy operation...");
        CompletableFuture.allOf(futures).join();
        taskList.clear();
    }

    private boolean verifyTheFileIsSame(final File srcFile, final File targetFile) {
        if (srcFile.length() != targetFile.length()) {
            return false;
        }

        try {
            if (!CheckSumUtil.getInstance().getFileChecksum(srcFile, messageDigestAlgo, blockSize)
                .equals(CheckSumUtil.getInstance().getFileChecksum(targetFile, messageDigestAlgo, blockSize))) {
                return false;
            }
        } catch (IOException | NoSuchAlgorithmException e) {
            logger.error(e);
            logger.error("Failed to compute the checksum, copying file again.");
            return false;
        }
        return true;
    }

    private boolean isMatchingExcludeFileTypes(String filePath) {
        if (excludeFileTypesSet != null) {
            final String extension = FileUtil.getExtensionByApacheCommonLib(filePath);
            return excludeFileTypesSet.contains(extension.toLowerCase());
        }
        return false;
    }

    private boolean isMatchingExcludePattern(String filePath) {
        if (excludePatternList != null) {
            for (final Pattern pattern : excludePatternList) {
                if (pattern.matcher(filePath).matches()) {
                    return true;
                }
            }
        }
        return false;
    }

    enum DateKeys {
        DECADE("Decade"),
        YEAR("Year"),
        MONTH("Month"),
        DAY("Day");

        final String value;

        DateKeys(String val) {
            this.value = val;
        }
    }

}
