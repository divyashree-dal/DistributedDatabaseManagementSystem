package com.group21.server.queries.constraints;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.logging.log4j.util.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.group21.configurations.ApplicationConfiguration;
import com.group21.constants.CommonRegex;
import com.group21.server.models.Column;
import com.group21.server.models.Constraint;
import com.group21.server.models.DataType;
import com.group21.server.models.DatabaseSite;
import com.group21.utils.FileReader;
import com.group21.utils.RegexUtil;

public class ConstraintCheck {
    private static final Logger LOGGER = LoggerFactory.getLogger(ConstraintCheck.class);

    public static boolean checkQueryConstraints(String tableName, String columnName, String columnValue, DatabaseSite databaseSite) {

        List<Column> columnList = databaseSite.readMetadata(tableName);
        List<String> columnNameList = new ArrayList<>();
        Map<String, DataType> columnTypeList = new HashMap<>();
        Map<String, Constraint> columnConstraintList = new HashMap<>();
        Map<String, String> columnForeignKeyTableList = new HashMap<>();
        Map<String, String> columnForeignKeyColumnNameList = new HashMap<>();

        for (Column c : columnList) {
            columnNameList.add(c.getColumnName());
            columnTypeList.put(c.getColumnName(), c.getColumnType());
            columnConstraintList.put(c.getColumnName(), c.getConstraint());
            columnForeignKeyTableList.put(c.getColumnName(), c.getForeignKeyTable());
            columnForeignKeyColumnNameList.put(c.getColumnName(), c.getForeignKeyColumnName());
        }

        if (!columnNameList.contains(columnName)) {
            LOGGER.error("Column Name '{}' does not exist in table", columnName);
            return false;
        }

        DataType columnValueDatatype = columnTypeList.get(columnName);
        if (columnValueDatatype.equals(DataType.INT)) {
            try {
                Integer.parseInt(columnValue);
            } catch (Exception e) {
                columnValue = "";
            }
        } else if (columnValueDatatype.equals(DataType.DOUBLE)) {
            try {
                Double.parseDouble(columnValue);
            } catch (Exception e) {
                columnValue = "";
            }
        } else if (columnValueDatatype.equals(DataType.TEXT)) {
            columnValue = RegexUtil.getMatch(columnValue, CommonRegex.TEXT_REGEX);

            if (columnValue != null) {
                columnValue = columnValue.substring(1, columnValue.length() - 1);
            }
        }
        if (Strings.isBlank(columnValue)) {
            LOGGER.error("Column '{}' requires value of type '{}'", columnName, columnValueDatatype.name());
            return false;
        }

        return true;
    }

    public static boolean checkForeignKeyConstraints(String tableName, List<String> uniqueIds, DatabaseSite databaseSite) {
        Map<String, DatabaseSite> gddMap = FileReader.readDistributedDataDictionary();
        List<Column> columnNameList;
        gddMap.remove(tableName);
        //Dict for foreign key check w.r.t tables
        for (String table : gddMap.keySet()) {
            DatabaseSite tableDatabaseSite = gddMap.get(table);
            if (ApplicationConfiguration.CURRENT_SITE == DatabaseSite.REMOTE) {
                if (tableDatabaseSite == DatabaseSite.REMOTE) {
                    tableDatabaseSite = DatabaseSite.LOCAL;
                } else {
                    continue;
                }
            }
            columnNameList = tableDatabaseSite.readMetadata(table);
            for (Column column : columnNameList) {
                if (column.getForeignKeyTable().equals(tableName)) {
                    List<String> violatedIds = checkForeignKeyUniqueIds(uniqueIds, table, column, tableDatabaseSite);
                    if (!violatedIds.isEmpty()) {
                        LOGGER.error("Foreign Key constraint violated for table '{}' with column '{}' and value '{}'", table, column.getColumnName(), String.join(",", violatedIds));
                        return true;
                    }
                }
            }
        }

        return false;
    }

    public static boolean checkForeignKeyTable(String tableName) {
        Map<String, DatabaseSite> gddMap = FileReader.readDistributedDataDictionary();
        List<Column> columnNameList;
        gddMap.remove(tableName);

        for (String table : gddMap.keySet()) {
            DatabaseSite tableDatabaseSite = gddMap.get(table);
            if (ApplicationConfiguration.CURRENT_SITE == DatabaseSite.REMOTE) {
                if (tableDatabaseSite == DatabaseSite.REMOTE) {
                    tableDatabaseSite = DatabaseSite.LOCAL;
                } else {
                    continue;
                }
            }
            columnNameList = tableDatabaseSite.readMetadata(table);
            for (Column column : columnNameList) {
                if (column.getForeignKeyTable().equals(tableName)) {
                    LOGGER.error("Foreign Key constraint violated for table '{}'", table);
                    return true;
                }
            }
        }

        return false;
    }

    private static List<String> checkForeignKeyUniqueIds(List<String> uniqueIds, String tableName, Column column, DatabaseSite databaseSite) {
        List<String> violatedIds = new ArrayList<>();
        List<String> fileLines = databaseSite.readData(tableName);
        fileLines.remove(0);
        for (String line : fileLines) {
            String[] columnList = line.split(ApplicationConfiguration.DELIMITER_REGEX);
            if (uniqueIds.contains(columnList[column.getColumnPosition()])) {
                violatedIds.add(columnList[column.getColumnPosition()]);
            }
        }
        return violatedIds;
    }

    public static boolean checkPrimaryKeyConstraints(String tableName, String uniqueId, DatabaseSite databaseSite) {
        List<Column> columns = databaseSite.readMetadata(tableName);

        List<Column> getPrimaryKeyColumns =
                columns.stream().filter(
                        t -> t.getConstraint().getKeyword().equals("PRIMARY KEY")
                ).collect(Collectors.toList());

        List<String> uniqueIds = new ArrayList<>();
        uniqueIds.add(uniqueId);

        List<String> idsPresent = checkForeignKeyUniqueIds(uniqueIds, tableName, getPrimaryKeyColumns.get(0), databaseSite);
        return idsPresent.isEmpty();

    }
}
