package com.group21.utils;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;

import org.apache.logging.log4j.util.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.group21.configurations.ApplicationConfiguration;
import com.group21.server.logger.EventLogger;
import com.group21.server.models.Column;
import com.group21.server.models.DatabaseSite;
import com.group21.server.models.TableInfo;

public class FileWriter {

    private static final Logger LOGGER = LoggerFactory.getLogger(FileWriter.class);

    private FileWriter() {
    }

    public static void writeFile(String fileName, String fileContent) {
        try {
            Path filePath = Paths.get(ApplicationConfiguration.DATA_DIRECTORY + ApplicationConfiguration.FILE_SEPARATOR + fileName);

            if (Files.notExists(filePath)) {
                Files.createFile(filePath);
            }

            Files.write(filePath, fileContent.getBytes(), StandardOpenOption.TRUNCATE_EXISTING);
        } catch (Exception exception) {
            LOGGER.error("Error occurred while writing file {}.", fileName);
            EventLogger.error(exception.getMessage());
        }
    }

    public static void writeMetadata(String tableName, List<Column> columnDetails) {
        StringBuilder tableMetadata = new StringBuilder(Strings.EMPTY);

        String headerRow = "ColumnName|ColumnType|Constraint|ForeignKeyTable|ForeignKeyColumn";
        tableMetadata.append(headerRow).append(ApplicationConfiguration.NEW_LINE);

        for (Column column : columnDetails) {
            StringJoiner columnEntry = new StringJoiner(ApplicationConfiguration.DELIMITER);

            columnEntry.add(column.getColumnName());
            columnEntry.add(column.getColumnType().name());
            columnEntry.add(column.getConstraint().name());
            columnEntry.add(column.getForeignKeyTable());
            columnEntry.add(column.getForeignKeyColumnName());

            tableMetadata.append(columnEntry.toString()).append(ApplicationConfiguration.NEW_LINE);
        }

        String metadataFileName = tableName + ApplicationConfiguration.METADATA_FILE_FORMAT;

        Path metadataFilePath = Paths.get(ApplicationConfiguration.DATA_DIRECTORY + ApplicationConfiguration.FILE_SEPARATOR + metadataFileName);

        try {
            if (Files.notExists(metadataFilePath)) {
                Files.createFile(metadataFilePath);
            }

            Files.write(metadataFilePath, tableMetadata.toString().getBytes());
        } catch (IOException exception) {
            LOGGER.error("Error occurred while storing table {} metadata.", tableName);
            EventLogger.error(exception.getMessage());
        }
    }

    public static void writeData(String tableName, List<String> columnData) {
        StringJoiner tableDataJoiner = new StringJoiner(ApplicationConfiguration.DELIMITER);

        for (String data : columnData) {
            tableDataJoiner.add(data);
        }

        String tableData = tableDataJoiner.toString() + ApplicationConfiguration.NEW_LINE;

        String dataFileName = tableName + ApplicationConfiguration.DATA_FILE_FORMAT;

        Path dataFilePath = Paths.get(ApplicationConfiguration.DATA_DIRECTORY + ApplicationConfiguration.FILE_SEPARATOR + dataFileName);

        try {
            if (Files.notExists(dataFilePath)) {
                Files.createFile(dataFilePath);
            }

            Files.write(dataFilePath, tableData.getBytes(), StandardOpenOption.APPEND);
        } catch (IOException exception) {
            LOGGER.error("Error occurred while storing data in table {}.", tableName);
            EventLogger.error(exception.getMessage());
        }
    }

    public static void writeLocalDataDictionary(TableInfo tableInfo) {
        try {
            Path localDDFilePath = Paths.get(ApplicationConfiguration.DATA_DIRECTORY + ApplicationConfiguration.FILE_SEPARATOR + ApplicationConfiguration.LOCAL_DATA_DICTIONARY_NAME);

            StringJoiner tableInfoJoiner = new StringJoiner(ApplicationConfiguration.DELIMITER);
            tableInfoJoiner.add(tableInfo.getTableName());
            tableInfoJoiner.add(String.valueOf(tableInfo.getNumberOfRows()));
            tableInfoJoiner.add(String.valueOf(tableInfo.getCreatedOn()));

            String tableInfoDetails = tableInfoJoiner.toString() + ApplicationConfiguration.NEW_LINE;

            Files.write(localDDFilePath, tableInfoDetails.getBytes(), StandardOpenOption.APPEND);
        } catch (IOException exception) {
            LOGGER.error("Error occurred while writing to local data dictionary.");
            EventLogger.error(exception.getMessage());
        }
    }

    public static void writeDistributedDataDictionary(String tableName, DatabaseSite databaseSite) {
        try {
            Path gddFilePath = Paths.get(ApplicationConfiguration.DATA_DIRECTORY + ApplicationConfiguration.FILE_SEPARATOR + ApplicationConfiguration.DISTRIBUTED_DATA_DICTIONARY_NAME);

            StringJoiner gddInfoJoiner = new StringJoiner(ApplicationConfiguration.DELIMITER);
            gddInfoJoiner.add(tableName);
            gddInfoJoiner.add(databaseSite.name());

            String tableInfoDetails = gddInfoJoiner.toString() + ApplicationConfiguration.NEW_LINE;

            Files.write(gddFilePath, tableInfoDetails.getBytes(), StandardOpenOption.APPEND);
        } catch (IOException exception) {
            LOGGER.error("Error occurred while writing to distributed data dictionary.");
            EventLogger.error(exception.getMessage());
        }
    }

    public static void incrementRowCountInLocalDataDictionary(String tableName) {
        List<TableInfo> tableInfoList = FileReader.readLocalDataDictionary();
        List<String> tableNameList = new ArrayList<>();
        for (TableInfo tableInfo1 : tableInfoList) {
            tableNameList.add(tableInfo1.getTableName());
        }
        TableInfo tableInfo = tableInfoList.get(tableNameList.indexOf(tableName));
        int rows = tableInfo.getNumberOfRows() + 1;
        tableInfo.setNumberOfRows(rows);
        tableInfoList.set(tableNameList.indexOf(tableName), tableInfo);
        Path localDDPath = Paths.get(ApplicationConfiguration.DATA_DIRECTORY + ApplicationConfiguration.FILE_SEPARATOR + ApplicationConfiguration.LOCAL_DATA_DICTIONARY_NAME);
        String headerRow = "TableName|NumberOfRows|CreatedOn" + ApplicationConfiguration.NEW_LINE;
        try {
            Files.write(localDDPath, headerRow.getBytes());
        } catch (IOException exception) {
            LOGGER.error("Error occurred while creating data directory.");
            EventLogger.error(exception.getMessage());
        }
        for (TableInfo tableInfo2 : tableInfoList) {
            writeLocalDataDictionary(tableInfo2);
        }
    }

    public static void decrementRowCountInLocalDataDictionary(String tableName, int count) {
        List<TableInfo> tableInfoList = FileReader.readLocalDataDictionary();
        List<String> tableNameList = new ArrayList<>();
        for (TableInfo tableInfo1 : tableInfoList) {
            tableNameList.add(tableInfo1.getTableName());
        }
        TableInfo tableInfo = tableInfoList.get(tableNameList.indexOf(tableName));
        int rows = tableInfo.getNumberOfRows() - count;
        tableInfo.setNumberOfRows(rows);
        tableInfoList.set(tableNameList.indexOf(tableName), tableInfo);
        Path localDDPath = Paths.get(ApplicationConfiguration.DATA_DIRECTORY + ApplicationConfiguration.FILE_SEPARATOR + ApplicationConfiguration.LOCAL_DATA_DICTIONARY_NAME);
        String headerRow = "TableName|NumberOfRows|CreatedOn" + ApplicationConfiguration.NEW_LINE;
        try {
            Files.write(localDDPath, headerRow.getBytes());
        } catch (IOException exception) {
            LOGGER.error("Error occurred while creating data directory.");
            EventLogger.error(exception.getMessage());
        }
        for (TableInfo tableInfo2 : tableInfoList) {
            writeLocalDataDictionary(tableInfo2);
        }
    }

    public static void deleteTable(String tableName) {
        try {
            Path dataFile = Paths.get(ApplicationConfiguration.DATA_DIRECTORY + ApplicationConfiguration.FILE_SEPARATOR + tableName + ApplicationConfiguration.DATA_FILE_FORMAT);
            Path metadataFile = Paths.get(ApplicationConfiguration.DATA_DIRECTORY + ApplicationConfiguration.FILE_SEPARATOR + tableName + ApplicationConfiguration.METADATA_FILE_FORMAT);

            Files.deleteIfExists(dataFile);
            Files.deleteIfExists(metadataFile);
        } catch (Exception exception) {
            LOGGER.error("Error occurred while deleting table {} files.", tableName);
            EventLogger.error(exception.getMessage());
        }
    }

    public static void deleteOnlyTable(String tableName) {
        try {
            Path dataFile = Paths.get(ApplicationConfiguration.DATA_DIRECTORY + ApplicationConfiguration.FILE_SEPARATOR + tableName + ApplicationConfiguration.DATA_FILE_FORMAT);

            Files.deleteIfExists(dataFile);
        } catch (Exception exception) {
            LOGGER.error("Error occurred while deleting table {} file.", tableName);
            EventLogger.error(exception.getMessage());
        }
    }

    public static String generateLocalDataDictionaryContent(List<TableInfo> tableInfoList) {
        String headerRow = "TableName|NumberOfRows|CreatedOn" + ApplicationConfiguration.NEW_LINE;
        StringBuilder tableInfoDetails = new StringBuilder(headerRow);
        for (TableInfo tableInfo : tableInfoList) {
            StringJoiner tableInfoJoiner = new StringJoiner(ApplicationConfiguration.DELIMITER);
            tableInfoJoiner.add(tableInfo.getTableName());
            tableInfoJoiner.add(String.valueOf(tableInfo.getNumberOfRows()));
            tableInfoJoiner.add(String.valueOf(tableInfo.getCreatedOn()));

            tableInfoDetails.append(tableInfoJoiner.toString()).append(ApplicationConfiguration.NEW_LINE);
        }
        return tableInfoDetails.toString();
    }

    public static String generateDistributedDataDictionaryContent(Map<String, DatabaseSite> gddMap) {
        String headerRow = "TableName|DatabaseSite" + ApplicationConfiguration.NEW_LINE;
        StringBuilder tableInfoDetails = new StringBuilder(headerRow);
        for (String tableName : gddMap.keySet()) {
            StringJoiner gddInfoJoiner = new StringJoiner(ApplicationConfiguration.DELIMITER);
            gddInfoJoiner.add(tableName);
            gddInfoJoiner.add(gddMap.get(tableName).name());

            tableInfoDetails.append(gddInfoJoiner.toString()).append(ApplicationConfiguration.NEW_LINE);
        }
        return tableInfoDetails.toString();
    }

    public static void writeTransactionFile(String query) {
        try {
            String fileContent = query + ApplicationConfiguration.NEW_LINE;
            Path transactionFile = Paths.get(ApplicationConfiguration.DATA_DIRECTORY + ApplicationConfiguration.FILE_SEPARATOR + ApplicationConfiguration.TRANSACTION_FILE_NAME);
            Files.write(transactionFile, fileContent.getBytes(), StandardOpenOption.APPEND);
        } catch (IOException exception) {
            LOGGER.error("Error occurred while writing to distributed data dictionary.");
            EventLogger.error(exception.getMessage());
        }
    }
}
