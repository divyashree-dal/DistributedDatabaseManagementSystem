package com.group21.client;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.group21.configurations.ApplicationConfiguration;
import com.group21.server.logger.EventLogger;
import com.group21.server.models.DatabaseSite;
import com.group21.utils.RemoteDatabaseReader;

public class DDBMSSetup {

    private static final Logger LOGGER = LoggerFactory.getLogger(DDBMSSetup.class);

    private DDBMSSetup() {
    }

    public static void perform() {
        Path dataDirectoryPath = Paths.get(ApplicationConfiguration.DATA_DIRECTORY);
        Path localDDPath = Paths.get(ApplicationConfiguration.DATA_DIRECTORY + ApplicationConfiguration.FILE_SEPARATOR + ApplicationConfiguration.LOCAL_DATA_DICTIONARY_NAME);
        Path distributedDDPath = Paths.get(ApplicationConfiguration.DATA_DIRECTORY + ApplicationConfiguration.FILE_SEPARATOR + ApplicationConfiguration.DISTRIBUTED_DATA_DICTIONARY_NAME);
        Path transactionFilePath = Paths.get(ApplicationConfiguration.DATA_DIRECTORY + ApplicationConfiguration.FILE_SEPARATOR + ApplicationConfiguration.TRANSACTION_FILE_NAME);

        Path generalLogFilePath = Paths.get(ApplicationConfiguration.LOG_DIRECTORY + ApplicationConfiguration.FILE_SEPARATOR + ApplicationConfiguration.GENERAL_LOG_FILE_NAME);
        Path eventLogFilePath = Paths.get(ApplicationConfiguration.LOG_DIRECTORY + ApplicationConfiguration.FILE_SEPARATOR + ApplicationConfiguration.EVENT_LOG_FILE_NAME);

        try {
            if (Files.notExists(dataDirectoryPath)) {
                Files.createDirectory(dataDirectoryPath);
            }

            if (Files.notExists(localDDPath)) {
                Files.createFile(localDDPath);

                String headerRow = "TableName|NumberOfRows|CreatedOn" + ApplicationConfiguration.NEW_LINE;
                Files.write(localDDPath, headerRow.getBytes());
            }

            if (Files.notExists(distributedDDPath)) {
                Files.createFile(distributedDDPath);

                String headerRow = "TableName|DatabaseSite" + ApplicationConfiguration.NEW_LINE;
                Files.write(distributedDDPath, headerRow.getBytes());
            }

            if (Files.notExists(transactionFilePath)) {
                Files.createFile(transactionFilePath);
            }

            if (Files.notExists(generalLogFilePath)) {
                Files.createFile(generalLogFilePath);
            }

            if (Files.notExists(eventLogFilePath)) {
                Files.createFile(eventLogFilePath);
            }

            if (ApplicationConfiguration.CURRENT_SITE == DatabaseSite.LOCAL) {
                // This is done only for local as remote site can not access local machine
                RemoteDatabaseReader.syncDistributedDataDictionary();
            }
        } catch (IOException exception) {
            LOGGER.error("Error occurred while creating data directory.");
            EventLogger.error(exception.getMessage());
        }
    }
}
