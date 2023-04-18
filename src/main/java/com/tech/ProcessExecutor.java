package com.tech;

import com.tech.utils.CheckSumUtil;
import com.tech.utils.FileUtil;
import com.tech.utils.StatsUtil;
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

    private static final String DEFAULT_FOLDER_SEQUENCE_PATH = "Decade->Series Title->Year->Episode Number;Episode Title";
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
    private final boolean shallowFileComparison;

    private Map<String, String> targetFileToSrcFileMap;

    public ProcessExecutor(Properties properties) {
        this.properties = properties;

        this.inputFile = this.properties.getProperty(PropKeysEnum.INPUT_FILE.name());
        this.sourceFolderPath = this.properties.getProperty(PropKeysEnum.SRC_FOLDER.name());
        this.targetFolderPath = this.properties.getProperty(PropKeysEnum.TARGET_FOLDER.name());
        String folderSequence = this.properties.getProperty(PropKeysEnum.FOLDER_SEQUENCE.name());

        initExcludeFileTypes(this.properties.getProperty(PropKeysEnum.EXCLUDE_FILE_TYPES.name()));
        initExcludePatterns(this.properties.getProperty(PropKeysEnum.EXCLUDE_PATTERNS.name()));

        replaceChars = this.properties.getProperty(PropKeysEnum.REPLACE_CHARS.name(), null);
        useStreamCopy = PropFileHandler.getBoolean(PropKeysEnum.USE_STREAM_COPY.name(), this.properties, true);
        blockSize = PropFileHandler.getInteger(PropKeysEnum.COPY_BLOCK_SIZE.name(), this.properties, CheckSumUtil.DEFAULT_BLOCK_SIZE);
        messageDigestAlgo = this.properties.getProperty(PropKeysEnum.CHECKSUM_SCHEME.name(), CheckSumUtil.DEFAULT_SCHEME);
        failFast = PropFileHandler.getBoolean(PropKeysEnum.FAIL_FAST.name(), this.properties, true);
        shallowFileComparison = PropFileHandler.getBoolean(PropKeysEnum.SHALLOW_FILE_COMPARISON.name(), this.properties, false);

        folderSequence = folderSequence == null || folderSequence.trim().isEmpty() ? DEFAULT_FOLDER_SEQUENCE_PATH : folderSequence;
        pathSequences = folderSequence.split("->");

        int nThreads = PropFileHandler.getInteger(PropKeysEnum.COPY_THREADS.name(), this.properties, 3);
        executorService = Executors.newFixedThreadPool(nThreads);

        targetFileToSrcFileMap = new HashMap<>();

        logger.info("Initializing executor with received args:\ninputFile {}\nsourceFolderPath {}\ntargetFolderPath {}\nfolder sequence {}",
            inputFile, sourceFolderPath, targetFolderPath, folderSequence);
    }

    private void initExcludePatterns(final String excludePatterns) {
        if (StringUtil.isNotBlank(excludePatterns)) {
            excludePatternList = new ArrayList<>();
            final String[] excludePatternArray = excludePatterns.split("->");
            Arrays.stream(excludePatternArray).forEach(type -> excludePatternList.add(Pattern.compile(type.trim(), Pattern.CASE_INSENSITIVE)));
        }
    }

    private void initExcludeFileTypes(final String excludeFileTypes) {
        if (StringUtil.isNotBlank(excludeFileTypes)) {
            excludeFileTypesSet = new HashSet<>();
            final String[] excludeFileTypeArray = excludeFileTypes.split("->");
            Arrays.stream(excludeFileTypeArray).forEach(type -> excludeFileTypesSet.add(type.trim().toLowerCase()));
        }
    }

    public void readTheExcelInputFile() throws IOException {
        Workbook workbook;
        Map<Integer, String> colIndexToHeaderMap = new HashMap<>();
        final int start_index = PropFileHandler.getInteger(PropKeysEnum.START_INDEX.name() + "_" + inputFile, this.properties, 0);
        int rowIndex = 0;

        if (start_index == 0) {
            takeBackUpOfExistingLogIfPresent();
            addStartEntryInLogFiles();
        }

        try (FileInputStream file = new FileInputStream(inputFile)) {
            workbook = new XSSFWorkbook(file);
            Sheet sheet = workbook.getSheetAt(0);
            readSheetAndStartFileCopy(colIndexToHeaderMap, start_index, rowIndex, sheet);
        }

        logger.info("Completed all tasks, calling final shutdown.");
        executorService.shutdown();
    }

    private void takeBackUpOfExistingLogIfPresent() {
        final File propsFilePath = DataOrganizerApplication.getPropsFilePath();
        if (propsFilePath.exists()) {
            FileUtil.rename(propsFilePath, propsFilePath.getName() + "-backup-" + new SimpleDateFormat("yyyy-MM-dd HH-mm-ss-SSS").format(new Date()));
        }
    }

    private void addStartEntryInLogFiles() {
        String startEntry = "";
        //FileUtil.appendEntryToLogFile(DataOrganizerApplication.getFailedFileLogPath(), startEntry, failFast);
        //FileUtil.appendEntryToLogFile(DataOrganizerApplication.getSkippedLogFile(), startEntry, failFast);
        FileUtil.appendEntryToLogFile(DataOrganizerApplication.getCopiedFileLogPath(), startEntry, failFast);
        StatsUtil.getInstance();
    }

    private void readSheetAndStartFileCopy(final Map<Integer, String> colIndexToHeaderMap, final int start_index, int rowIndex, final Sheet sheet) throws IOException {
        int colIndex;
        Map<String, String> colKeyValueMapInCurrentRow;
        for (Row row : sheet) {
            rowIndex++;
            if (rowIndex != 1 && rowIndex <= start_index) {
                continue;
            }
            colIndex = 0;
            colKeyValueMapInCurrentRow = new HashMap<>();
            iterateOverAllColsInRowToStoreInKeyValuePair(colIndexToHeaderMap, rowIndex, colIndex, colKeyValueMapInCurrentRow, row);
            if (rowIndex > 1) {
                processCopyOperationOnGivenRow(colKeyValueMapInCurrentRow, rowIndex);
                updatePropertiesFileWithStartIndex(rowIndex);
            }
        }
    }

    private void updatePropertiesFileWithStartIndex(final int rowIndex) throws IOException {
        PropFileHandler.setProperty(PropKeysEnum.START_INDEX.name() + "_" + inputFile, rowIndex + "", this.properties);
        PropFileHandler.flush(this.properties, DataOrganizerApplication.getPropFilePath());
    }

    private static void iterateOverAllColsInRowToStoreInKeyValuePair(final Map<Integer, String> colIndexToHeaderMap, final int rowIndex, int colIndex, final Map<String, String> colKeyValueMapInCurrentRow, final Row row) {
        for (Cell cell : row) {
            CellType cellType = cell.getCellType();
            colIndex++;
            if (rowIndex == 1) {
                colIndexToHeaderMap.put(colIndex, cell.getStringCellValue().trim());
                continue;
            }
            handleColumnValueAndStoreInKeyValueMap(colKeyValueMapInCurrentRow, colIndexToHeaderMap, colIndex, cell, cellType);
        }
    }

    private static void handleColumnValueAndStoreInKeyValueMap(final Map<String, String> rowKeyValueMap, final Map<Integer, String> colIndexToHeaderMap,
                                                               final int colIndex, final Cell cell,
                                                               final CellType cellType) {
        if (Objects.requireNonNull(cellType) == CellType.NUMERIC && DateUtil.isCellDateFormatted(cell)) {
            breakDateFieldAndStoreInKeyValueMap(rowKeyValueMap, cell);
        } else if (Objects.requireNonNull(cellType) == CellType.NUMERIC) {
            rowKeyValueMap.put(colIndexToHeaderMap.get(colIndex), decimalFormat.format(cell.getNumericCellValue()) + "");
        } else if (cellType == CellType.STRING) {
            rowKeyValueMap.put(colIndexToHeaderMap.get(colIndex), cell.getStringCellValue().trim());
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
        copyFilesFromSourceToTarget(new File(sourceFolderPath, rowKeyValueMap.get(DEFAULT_GUID_NAME)), targetFolder, taskList, rowIndex);

        if (!taskList.isEmpty()) {
            logger.info("Awaiting copy operation for row {} to be completed.", rowIndex);
            executeTaskList(taskList);
            StatsUtil.getInstance().flushChanges();
            logger.info("Completed copy operation for row {}.", rowIndex);
        }
    }

    private File createFolderStructureIfNeeded(String[] pathSequences, Map<String, String> rowEntryKeyValuePair, String outputFolderPath) throws IOException {
        File folderPathToBeCreated = new File(outputFolderPath);
        folderPathToBeCreated = iterateOverPathSequenceToAppendPath(pathSequences, rowEntryKeyValuePair, folderPathToBeCreated);
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

    private File iterateOverPathSequenceToAppendPath(final String[] pathSequences, final Map<String, String> rowEntryKeyValuePair, File folderPathToBeCreated) {
        for (final String pathSequence : pathSequences) {
            final String[] paths = pathSequence.trim().split(";");
            StringBuilder newChildName = null;
            for (String pathKey : paths) {
                String pathValue = rowEntryKeyValuePair.get(pathKey);
                if (pathValue == null) {
                    continue;
                }
                if (replaceChars != null) {
                    pathValue = pathValue.replaceAll(replaceChars, "");
                }
                if (newChildName == null) {
                    newChildName = new StringBuilder();
                    newChildName.append(pathValue);
                } else {
                    newChildName.append(" ");
                    newChildName.append(pathValue);
                }
            }
            if (newChildName != null) {
                folderPathToBeCreated = new File(folderPathToBeCreated, newChildName.toString());
            }
        }
        return folderPathToBeCreated;
    }

    /**
     * @param srcFolder    folder from where files to be copied
     * @param targetFolder target folder where files need to be copied
     * @param taskList     taskList for executing completable future
     * @throws IOException throw exception if any
     */
    private void copyFilesFromSourceToTarget(File srcFolder, File targetFolder, List<Runnable> taskList, int rowIndex) throws IOException {
        if (!srcFolder.exists()) {
            logger.error("Source folder {} is not present, skipping the row index {} for it ", srcFolder.getPath(), rowIndex);
            return;
        }
        Files.walkFileTree(srcFolder.toPath(), new SimpleFileVisitor<Path>() {
            @Override
            public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) {
                if (isMatchingExcludePattern(dir.toFile().getPath())) {
                    logger.info("skipping sub-path as matched to exclude pattern {}", dir.toFile().getPath());
                    FileUtil.appendEntryToLogFile(DataOrganizerApplication.getSkippedLogFile(), dir.toFile().getPath() + "\n", failFast);
                    StatsUtil.getInstance().updateFolderStats(true);
                    return FileVisitResult.SKIP_SUBTREE;
                }
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) {
                if (!Files.isDirectory(file)) {
                    if (isMatchingExcludePattern(file.toFile().getPath()) || isMatchingExcludeFileTypes(file.toFile().getPath())) {
                        logger.info("skipping file {} as per exclude pattern and file types", file.toFile().getPath());
                        FileUtil.appendEntryToLogFile(DataOrganizerApplication.getSkippedLogFile(), file.toFile().getPath() + "\n", failFast);
                        StatsUtil.getInstance().updateStats(0, false, true, false);
                        return FileVisitResult.CONTINUE;
                    }
                    File targetFile = getTargetFile(file, targetFolder);

                    if (checkIfFileAlreadyExists(targetFile)) {
                        logger.info("File {} with same name already present at target {}", file.toFile().getPath(), targetFile.getPath());
                        //File targetFileInst = targetFile.exists() ? targetFile : new File(targetFileToSrcFileMap.get(targetFile.getPath()));
                       /* if (verifyTheFileIsSame(file.toFile(), targetFileInst)) {
                            //Same file already copied, so skip this one
                            logger.info("Skipping the duplicate file {}", file.toFile().getPath());
                            FileUtil.appendEntryToLogFile(DataOrganizerApplication.getDuplicateLogFile(), file.toFile().getPath(), failFast);
                            StatsUtil.getInstance().updateDupFile();
                            return FileVisitResult.CONTINUE;
                        } else {*/
                        // file with same name already present, so rename this one.
                        int counter = 0;
                        String oldPath = targetFile.getPath();
                        while (targetFile.exists() || targetFileToSrcFileMap.containsKey(targetFile.getPath())) {
                            counter += 1;
                            targetFile = FileUtil.appendSuffix(new File(oldPath), "-" + counter);
                        }
                        logger.info("Renaming the target file {} with {}", oldPath, targetFile.getPath());
                        //}

                    }
                    addNewCopyTask(file, targetFile, taskList, srcFolder);
                }
                return FileVisitResult.CONTINUE;
            }
        });
    }

    private void addNewCopyTask(final Path file, final File targetFile, final List<Runnable> taskList, final File srcFolder) {
        targetFileToSrcFileMap.put(targetFile.getPath(), file.toFile().getPath());
        taskList.add(new CopyFileTask(file.toFile(), targetFile, blockSize, useStreamCopy, failFast));
        if (taskList.size() >= 1000) {
            executeTaskList(taskList);
            logger.info("Completed copy operation on batch on files inside {}", srcFolder.getPath());
        }
    }

    private boolean checkIfFileAlreadyExists(final File targetFile) {
        return (targetFile.exists() || targetFileToSrcFileMap.containsKey(targetFile.getPath()));
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
        targetFileToSrcFileMap.clear();
    }

    private boolean verifyTheFileIsSame(final File srcFile, final File targetFile) {
        if (srcFile.length() != targetFile.length()) {
            return false;
        }
        try {
            if (!this.shallowFileComparison && !CheckSumUtil.getInstance().getFileChecksum(srcFile, messageDigestAlgo, blockSize)
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
