package com.tech;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.util.Properties;

import static com.tech.PropKeysEnum.FOLDER_SEQUENCE;
import static com.tech.PropKeysEnum.INPUT_FILE;
import static com.tech.PropKeysEnum.SRC_FOLDER;
import static com.tech.PropKeysEnum.TARGET_FOLDER;


/*todo
* Add integrity check to compare the source and target folder.
* Don't stop script on error -- it must keep on running
* Keep record of files -
*  Which got successfully migrated
*  Which got failed to migrate
* */

public class DataOrganizerApplication {

    static Logger logger = LogManager.getLogger(DataOrganizerApplication.class);

    private static final String PROP_FILE_NAME = "organizer.properties";
    private static final String TEMP = "temp";
    private static final String DATA_ORG = "data-organizer";
    private static final String RESOURCES = "resources";

    private static final String LOGS = "logs";
    private static final String FILES_FAILED = "files-failed.txt";

    private final Properties properties;

    private DataOrganizerApplication(Properties properties) {
        this.properties = properties;
    }

    private void startDataOrganizeProcess() {
        ProcessExecutor processExecutor = new ProcessExecutor(properties.getProperty(INPUT_FILE.name()),
            properties.getProperty(SRC_FOLDER.name()),
            properties.getProperty(TARGET_FOLDER.name()),
            properties.getProperty(FOLDER_SEQUENCE.name()), properties);
        try {
            processExecutor.readTheExcelInputFile();
        } catch (IOException | InterruptedException e) {
            logger.error(e);
            System.exit(1);
        }
    }

    public static void main(String[] args) throws IOException {
        logger.info("\n\n\n******* Starting Application ****\n\n\n");
        Properties properties;
        if (args.length == 0) {
            File propFilePath = getPropFilePath();
            properties = PropFileHandler.readPropertiesFile(propFilePath.getPath());
        } else {
            properties = new Properties();
            int i = 0;
            for (String arg : args) {
                arg = arg.toUpperCase().trim();
                i++;
                final PropKeysEnum argEnum = PropKeysEnum.valueOf(arg);
                switch (argEnum) {
                    case HELP:
                        showHelp();
                        break;
                    case INPUT_FILE:
                        properties.put(INPUT_FILE.name(), args[i++]);
                        break;
                    case PROP_FILE:
                        properties.put(PropKeysEnum.PROP_FILE.name(), args[i++]);
                        break;
                    case SRC_FOLDER:
                        properties.put(PropKeysEnum.SRC_FOLDER.name(), args[i++]);
                        break;
                    case TARGET_FOLDER:
                        properties.put(PropKeysEnum.TARGET_FOLDER.name(), args[i++]);
                        break;
                    case FOLDER_SEQUENCE:
                        properties.put(PropKeysEnum.FOLDER_SEQUENCE.name(), args[i++]);
                        break;
                    case COPY_BLOCK_SIZE:
                        properties.put(PropKeysEnum.COPY_BLOCK_SIZE.name(), args[i++]);
                        break;
                    case USE_STREAM_COPY:
                        properties.put(PropKeysEnum.USE_STREAM_COPY.name(), args[i++]);
                        break;
                    case EXCLUDE_FILE_TYPES:
                        properties.put(PropKeysEnum.EXCLUDE_FILE_TYPES.name(), args[i++]);
                        break;
                    case EXCLUDE_PATTERNS:
                        properties.put(PropKeysEnum.EXCLUDE_PATTERNS.name(), args[i++]);
                        break;
                    case COPY_THREADS:
                        properties.put(PropKeysEnum.COPY_THREADS.name(), args[i++]);
                        break;
                    case REPLACE_CHARS:
                        properties.put(PropKeysEnum.REPLACE_CHARS.name(), args[i++]);
                        break;
                    case CHECKSUM_SCHEME:
                        properties.put(PropKeysEnum.CHECKSUM_SCHEME.name(), args[i++]);
                        break;
                    case FAIL_FAST:
                        properties.put(PropKeysEnum.FAIL_FAST.name(), args[i++]);
                        break;
                    default:
                        throw new IllegalStateException("Unexpected value: " + arg);
                }
            }
        }

        if (properties.isEmpty()) {
            showHelp();
        } else {
            new DataOrganizerApplication(properties).startDataOrganizeProcess();
        }
    }

    public static File getPropFilePath() {
        final String homeDrive = System.getProperty("user.home");
        return new File(homeDrive, TEMP + File.separator + DATA_ORG + File.separator + RESOURCES + File.separator + PROP_FILE_NAME);
    }

    public static File getFailedFileLogPath() {
        final String homeDrive = System.getProperty("user.home");
        return new File(homeDrive, TEMP + File.separator + DATA_ORG + File.separator + LOGS + File.separator + FILES_FAILED);
    }

    private static void showHelp() {
        printConsoleLog("Run the executable using the following command line arguments: \n");
        printConsoleLog("INPUT_FILE*        -- Excel file path containing the details of source files and target folder");
        printConsoleLog("PROP_FILE          -- properties file path containing all the needed properties key values.\n\t\t\t\t\t\tDefault path for properties file is " + getPropFilePath());
        printConsoleLog("SRC_FOLDER*        -- Source folder path which needs to be copied into a organised structure");
        printConsoleLog("TARGET_FOLDER*     -- Target folder path where needs to be copy the files into organised structure");
        printConsoleLog("FOLDER_SEQUENCE    -- Customise the folder path to be created target folder based on column values in excel.\n\t\t\t\t\t\tDefault path is Decade->Series Title->Year->Episode Number");
        printConsoleLog("COPY_BLOCK_SIZE    -- Block size used for copy file using streams\n\t\t\t\t\t\tDefault value is 4096");
        printConsoleLog("USE_STREAM_COPY    -- Use buffered streams to copy file else will use the Java Files.copy.\n\t\t\t\t\t\tDefault value is true");
        printConsoleLog("EXCLUDE_FILE_TYPES -- Specify the file types to be skipped e.g srt->png");
        printConsoleLog("EXCLUDE_PATTERNS   -- Specify the patterns to be skipped e.g .*h264.mov->.*h264.mpg");
        printConsoleLog("COPY_THREADS       -- Number of parallel threads for copy files\n\t\t\t\t\t\tDefault value is 3");
        printConsoleLog("CHECKSUM_SCHEME    -- Checksum algorithm for validating file before replace.\n\t\t\t\t\t\tDefault is SHA-256, valid algorithms are SHA-1,SHA-256,MD5,CRC32");
        printConsoleLog("REPLACE_CHARS      -- Regex patterns to replace special characters from file names e.g [!@#$%^&]");
        printConsoleLog("FAIL_FAST          -- Fail fast as script hits any error else it will just log the error and keep trying the next paths.\n\t\t\t\t\tDefault is true");

        printConsoleLog("\n\nCreate file at path " + getPropFilePath() + " and add the required properties key value pairs.\n\n");

        System.exit(0);
    }

    private static void printConsoleLog(String msg) {
        System.out.println(msg);
    }

}
