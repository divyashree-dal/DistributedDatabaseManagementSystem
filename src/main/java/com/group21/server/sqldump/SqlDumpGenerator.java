package com.group21.server.sqldump;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.group21.configurations.ApplicationConfiguration;
import com.group21.server.logger.EventLogger;
import com.group21.server.models.Column;
import com.group21.server.models.Constraint;
import com.group21.server.models.DataType;
import com.group21.server.models.DatabaseSite;
import com.group21.server.models.TableInfo;
import com.group21.server.queries.createtable.CreateTableParser;
import com.group21.utils.FileReader;
import com.group21.utils.RemoteDatabaseReader;

public class SqlDumpGenerator {

    public static final String START_BRACKET = "(";
    public static final String CLOSE_BRACKET = ")";

    private static final Logger LOGGER = LoggerFactory.getLogger(SqlDumpGenerator.class);

    public static void generate() {
        Path sqlDumpFilePath = Paths.get(ApplicationConfiguration.DATA_DIRECTORY + ApplicationConfiguration.FILE_SEPARATOR + ApplicationConfiguration.SQL_DUMP_FILE_NAME);

        try {
            if (Files.notExists(sqlDumpFilePath)) {
                Files.createFile(sqlDumpFilePath);
            } else {
                Files.write(sqlDumpFilePath, "".getBytes(), StandardOpenOption.TRUNCATE_EXISTING);
            }

            Map<String, DatabaseSite> tableInfoMap = FileReader.readDistributedDataDictionary();

            List<TableInfo> localTableList = FileReader.readLocalDataDictionary();
            Map<String, TableInfo> localTableInfoMap = new HashMap<>();
            for (TableInfo tableInfo : localTableList) {
                localTableInfoMap.put(tableInfo.getTableName(), tableInfo);
            }

            List<TableInfo> remoteTableList = RemoteDatabaseReader.readLocalDataDictionary();
            Map<String, TableInfo> remoteTableInfoMap = new HashMap<>();
            for (TableInfo tableInfo : remoteTableList) {
                remoteTableInfoMap.put(tableInfo.getTableName(), tableInfo);
            }

            Map<Long, String> queriesByCreationDate = new TreeMap<>();

            Set<String> tableNameList = tableInfoMap.keySet();
            for (String tableName : tableNameList) {
                DatabaseSite site = tableInfoMap.get(tableName);

                StringBuilder queryString = new StringBuilder("CREATE TABLE " + tableName.toLowerCase() + " " + START_BRACKET);

                List<Column> localColumns = new ArrayList<>();
                if (site.equals(DatabaseSite.LOCAL)) {
                    localColumns = FileReader.readMetadata(tableName);
                } else if (site.equals(DatabaseSite.REMOTE)) {
                    localColumns = RemoteDatabaseReader.readMetadata(tableName);
                }

                List<String> columnNameList = localColumns.stream().map(Column::getColumnName).collect(Collectors.toList());
                List<DataType> dataTypeList = localColumns.stream().map(Column::getColumnType).collect(Collectors.toList());
                List<Constraint> constraintList = localColumns.stream().map(Column::getConstraint).collect(Collectors.toList());
                List<String> foreignKeyTableList = localColumns.stream().map(Column::getForeignKeyTable).collect(Collectors.toList());
                List<String> foreignKeyColumnNameList = localColumns.stream().map(Column::getForeignKeyColumnName).collect(Collectors.toList());
                List<Integer> columnPositionList = localColumns.stream().map(Column::getColumnPosition).collect(Collectors.toList());

                for (int i = 0; i < columnPositionList.size(); i++) {

                    queryString.append(columnNameList.get(i).toLowerCase()).append(" ");
                    if (!dataTypeList.get(i).equals(DataType.UNKNOWN)) {
                        queryString.append(dataTypeList.get(i));
                    }

                    if (!constraintList.get(i).equals(Constraint.UNKNOWN)) {
                        if (constraintList.get(i).equals(Constraint.PRIMARY_KEY)) {
                            queryString.append(" primary key");
                        }

                        if (constraintList.get(i).equals(Constraint.FOREIGN_KEY)) {
                            queryString.append(" foreign key references ");
                        }
                    }

                    if (!foreignKeyTableList.get(i).equals("null")) {
                        queryString.append(foreignKeyTableList.get(i).toLowerCase());
                        queryString.append(START_BRACKET).append(foreignKeyColumnNameList.get(i).toLowerCase()).append(CLOSE_BRACKET);
                    }

                    if (i == columnPositionList.size() - 1) {
                        queryString.append(CLOSE_BRACKET);
                    } else {
                        queryString.append(", ");
                    }
                }

                TableInfo tableInfo;
                if (site == DatabaseSite.LOCAL) {
                    tableInfo = localTableInfoMap.get(tableName);
                } else {
                    tableInfo = remoteTableInfoMap.get(tableName);
                }

                long createdOn;
                if (tableInfo != null) {
                    createdOn = tableInfo.getCreatedOn();
                } else {
                    createdOn = 0;
                }

                queryString.append(";").append(ApplicationConfiguration.NEW_LINE);

                queriesByCreationDate.put(createdOn, queryString.toString());
            }

            for (String query : queriesByCreationDate.values()) {
                CreateTableParser parser = new CreateTableParser();
                String tableName = parser.getTableName(query);

                String queryComment = "-- Create table query for '" + tableName + "' table";

                String queryContent = queryComment + ApplicationConfiguration.NEW_LINE + query + ApplicationConfiguration.NEW_LINE;
                Files.write(sqlDumpFilePath, queryContent.getBytes(), StandardOpenOption.APPEND);
            }

            LOGGER.info("SqlDump file '{}' generated successfully.", ApplicationConfiguration.SQL_DUMP_FILE_NAME);
        } catch (IOException exception) {
            LOGGER.info("Error occurred while generating Sql Ddump.");
            EventLogger.error(exception.getMessage());
        }
    }
}
