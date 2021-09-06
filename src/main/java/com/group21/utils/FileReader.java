package com.group21.utils;

import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.group21.configurations.ApplicationConfiguration;
import com.group21.server.logger.EventLogger;
import com.group21.server.models.Column;
import com.group21.server.models.Constraint;
import com.group21.server.models.DataType;
import com.group21.server.models.DatabaseSite;
import com.group21.server.models.TableInfo;

public class FileReader {

    private static final Logger LOGGER = LoggerFactory.getLogger(FileReader.class);

    private FileReader() {
    }

    public static Map<String, String> readAuthenticationFile() {
        String authenticationFileName = ApplicationConfiguration.AUTHENTICATION_FILE_NAME;

        Map<String, String> authenticationMap = new HashMap<>();
        try {
            URL authenticationFileUrl = FileReader.class.getClassLoader().getResource(authenticationFileName);

            assert authenticationFileUrl != null;

            Path authenticationFilePath = Paths.get(authenticationFileUrl.toURI());

            List<String> fileLines = Files.readAllLines(authenticationFilePath);

            for (String line : fileLines) {
                String[] columnList = line.split(ApplicationConfiguration.DELIMITER_REGEX);
                String username = columnList[0];
                String password = columnList[1];

                authenticationMap.put(username, password);
            }
        } catch (URISyntaxException | IOException exception) {
            LOGGER.error("Error occurred while reading authentication file.");
            EventLogger.error(exception.getMessage());
        }
        return authenticationMap;
    }

    public static List<TableInfo> readLocalDataDictionary() {
        List<TableInfo> tableInfoList = new ArrayList<>();
        try {
            Path localDDFilePath = Paths.get(ApplicationConfiguration.DATA_DIRECTORY + ApplicationConfiguration.FILE_SEPARATOR + ApplicationConfiguration.LOCAL_DATA_DICTIONARY_NAME);
            List<String> fileLines = Files.readAllLines(localDDFilePath);
            fileLines.remove(0);

            for (String line : fileLines) {
                String[] columnList = line.split(ApplicationConfiguration.DELIMITER_REGEX);

                TableInfo tableInfo = new TableInfo();
                tableInfo.setTableName(columnList[0]);
                tableInfo.setNumberOfRows(Integer.parseInt(columnList[1]));
                tableInfo.setCreatedOn(Long.parseLong(columnList[2]));

                tableInfoList.add(tableInfo);
            }
        } catch (IOException exception) {
            LOGGER.error("Error occurred while reading local data dictionary.");
            EventLogger.error(exception.getMessage());
        }
        return tableInfoList;
    }


    public static Map<String, DatabaseSite> readDistributedDataDictionary() {
        Map<String, DatabaseSite> tableInfoMap = new LinkedHashMap<>();
        try {
            if (ApplicationConfiguration.CURRENT_SITE == DatabaseSite.LOCAL) {
                // This is done only for local as remote site can not access local machine
                RemoteDatabaseReader.syncDistributedDataDictionary();
            }
            Path localDDFilePath = Paths.get(ApplicationConfiguration.DATA_DIRECTORY + ApplicationConfiguration.FILE_SEPARATOR + ApplicationConfiguration.DISTRIBUTED_DATA_DICTIONARY_NAME);
            List<String> fileLines = Files.readAllLines(localDDFilePath);
            fileLines.remove(0);

            for (String line : fileLines) {
                String[] columnList = line.split(ApplicationConfiguration.DELIMITER_REGEX);

                tableInfoMap.put(columnList[0], DatabaseSite.from(columnList[1]));
            }
        } catch (IOException exception) {
            LOGGER.error("Error occurred while reading distributed data dictionary.");
            EventLogger.error(exception.getMessage());
        }
        return tableInfoMap;
    }

    public static List<String> readData(String tableName) {
        String dataFileName = tableName + ApplicationConfiguration.DATA_FILE_FORMAT;
        Path localDataFilePath = Paths.get(ApplicationConfiguration.DATA_DIRECTORY + ApplicationConfiguration.FILE_SEPARATOR + dataFileName);
        List<String> fileLines = new ArrayList<>();
        try {
            fileLines = Files.readAllLines(localDataFilePath);
        } catch (IOException exception) {
            EventLogger.error(exception.getMessage());
        }
        return fileLines;
    }

    public static List<String> readColumnMetadata(String tableName) {
        String metadataFileName = tableName + ApplicationConfiguration.METADATA_FILE_FORMAT;
        Path localDDFilePath = Paths.get(ApplicationConfiguration.DATA_DIRECTORY + ApplicationConfiguration.FILE_SEPARATOR + metadataFileName);
        List<String> fileLines;
        List<String> columnNames = new ArrayList<>();
        try {
            fileLines = Files.readAllLines(localDDFilePath);
            fileLines.remove(0);

            for (String line : fileLines) {
                String[] columnList = line.split(ApplicationConfiguration.DELIMITER_REGEX);
                columnNames.add(columnList[0]);
            }
        } catch (IOException e) {
            LOGGER.error("Error occurred while reading column name meta data");
            EventLogger.error(e.getMessage());
        }
        return columnNames;
    }

    public static List<Column> readMetadata(String tableName) {
        List<Column> columnInfoList = new ArrayList<>();
        try {
            Path localDDFilePath = Paths.get(ApplicationConfiguration.DATA_DIRECTORY + ApplicationConfiguration.FILE_SEPARATOR + tableName + ApplicationConfiguration.METADATA_FILE_FORMAT);
            List<String> fileLines = Files.readAllLines(localDDFilePath);
            fileLines.remove(0);
            Integer count = 0;
            for (String line : fileLines) {
                String[] columnInfo = line.split(ApplicationConfiguration.DELIMITER_REGEX);
                Column column = new Column();
                column.setColumnName(columnInfo[0]);
                column.setColumnType(DataType.valueOf(columnInfo[1]));
                column.setConstraint(Constraint.valueOf(columnInfo[2]));
                column.setForeignKeyTable(columnInfo[3]);
                column.setForeignKeyColumnName(columnInfo[4]);
                column.setColumnPosition(count++);

                columnInfoList.add(column);
            }
        } catch (IOException exception) {
            LOGGER.error("Error occurred while reading metadata.");
            EventLogger.error(exception.getMessage());
        }
        return columnInfoList;
    }

    public static List<String> readColumnData(String tableName, String columnName) {
        List<String> columnDataList = new ArrayList<>();
        try {
            Path localDDFilePath = Paths.get(ApplicationConfiguration.DATA_DIRECTORY + ApplicationConfiguration.FILE_SEPARATOR + tableName + ApplicationConfiguration.DATA_FILE_FORMAT);
            List<String> fileLines = Files.readAllLines(localDDFilePath);

            int index = 0;
            String firstLine = fileLines.get(0);
            String[] firstLineArray = firstLine.split(ApplicationConfiguration.DELIMITER_REGEX);

            for (int i = 0; i < firstLineArray.length; i++) {
                if (columnName.equals(firstLineArray[i])) {
                    index = i;
                    break;
                }
            }

            fileLines.remove(0);

            for (String line : fileLines) {
                String[] columnData = line.split(ApplicationConfiguration.DELIMITER_REGEX);
                columnDataList.add(columnData[index]);
            }

        } catch (IOException exception) {
            LOGGER.error("Error occurred while reading column data.");
            EventLogger.error(exception.getMessage());
        }
        return columnDataList;
    }

    public static List<String> readTransactionFile() {
        List<String> queries = null;
        try {
            Path transactionFile = Paths.get(ApplicationConfiguration.DATA_DIRECTORY + ApplicationConfiguration.FILE_SEPARATOR + ApplicationConfiguration.TRANSACTION_FILE_NAME);
            queries = Files.readAllLines(transactionFile);
        } catch (IOException exception) {
            LOGGER.error("Error occurred while writing to distributed data dictionary.");
            EventLogger.error(exception.getMessage());
        }
        return queries;
    }
}
