package com.tech;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.poi.util.StringUtil;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Properties;

class PropFileHandler {

    static Logger logger = LogManager.getLogger(PropFileHandler.class);

    private PropFileHandler() {
    }

    public static Properties readPropertiesFile(String filePath) throws IOException {
        Properties prop = new Properties();
        if (new File(filePath).exists()) {
            logger.info("Starting reading prop file {}", filePath);
            try (InputStream input = Files.newInputStream(Paths.get(filePath))) {
                prop.load(input);

            } catch (IOException ex) {
                throw new IOException(ex);
            }
        } else {
            logger.info("Properties file path {} does not exists or have enough permissions", filePath);
        }
        return prop;
    }

    public static void setProperty(String key, String value, Properties properties) {
       properties.setProperty(key, value);
    }

    public static synchronized void flush(Properties properties, File outPutFilePath) throws IOException {
        try (final OutputStream outputstream
                 = Files.newOutputStream(outPutFilePath.toPath())) {
            properties.store(outputstream, "File Updated");
        }
    }

    public static int getInteger(String propertyName,
                                 Properties properties, int defaultValue) {
        String value = extractPropertyValue(propertyName, properties);
        return value == null ? defaultValue : Integer.parseInt(value);
    }

    public static boolean getBoolean(String propertyName,
                                     Properties properties, boolean defaultValue) {
        final Object valueObject = getPropertyValueObject(propertyName, properties, defaultValue);
        try {
            return Boolean.parseBoolean(String.valueOf(valueObject));
        } catch (Exception e) {
            return defaultValue;
        }
    }

    private static Object getPropertyValueObject(final String propertyName, final Properties properties, final Object defValue) {
        return properties.getOrDefault(propertyName, defValue);
    }

    public static String extractPropertyValue(String propertyName,
                                              Properties properties) {
        String value = properties.getProperty(propertyName);
        if (value == null) {
            return null;
        }
        value = value.trim();
        if (StringUtil.isBlank(value)) {
            return null;
        }
        return value;
    }
}
