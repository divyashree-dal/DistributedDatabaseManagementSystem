package com.group21.server.queries.delete;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.logging.log4j.util.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.group21.configurations.ApplicationConfiguration;
import com.group21.server.logger.EventLogger;
import com.group21.server.models.Column;
import com.group21.server.models.Constraint;
import com.group21.server.models.DatabaseSite;
import com.group21.server.models.TableInfo;
import com.group21.server.queries.constraints.ConstraintCheck;
import com.group21.utils.FileWriter;
import com.group21.utils.RegexUtil;

public class DeleteParser {
    private static final Logger LOGGER = LoggerFactory.getLogger(DeleteParser.class);
    private static final String DELETE_TABLE_REGEX = "DELETE FROM [a-zA-Z_]+;?";
    private static final String DELETE_TABLE_WHERE_REGEX = "DELETE FROM [a-zA-Z_]+ WHERE [a-zA-Z_]+ (=) '?[a-zA-Z0-9_]+'?;?";

    public boolean isValid(String query) {
        String matchedQuery = RegexUtil.getMatch(query, DELETE_TABLE_REGEX);
        String matchedWhereQuery = RegexUtil.getMatch(query, DELETE_TABLE_WHERE_REGEX);
        if (Strings.isBlank(matchedQuery) && Strings.isBlank(matchedWhereQuery)) {
            LOGGER.error("Syntax error in provided delete query!");
            return false;
        }
        return true;
    }

    public boolean isWhereConditionExists(String query) {
        return query.matches(DELETE_TABLE_WHERE_REGEX);
    }

    public String getTableName(String query) {

        int indexOfFirstSpace = query.indexOf(' ');
        int indexOfSecondSpace = query.indexOf(' ', indexOfFirstSpace + 1);

        if (isWhereConditionExists(query)) {
            int indexOfThirdSpace = query.indexOf(' ', indexOfSecondSpace + 1);

            return query.substring(indexOfSecondSpace + 1, indexOfThirdSpace).trim();
        } else {
            return query.substring(indexOfSecondSpace + 1).replace(";", "").trim();
        }
    }

    public boolean deleteTable(TableInfo tableInfo, String query, DatabaseSite databaseSite, boolean isAutoCommit) {
        try {
            List<String> fileLines = databaseSite.readData(tableInfo.getTableName());
            List<String> uniqueIds = new ArrayList<>();
            fileLines.remove(0);

            List<Column> columns = databaseSite.readMetadata(tableInfo.getTableName());

            Column primaryColumn = null;
            for (Column column : columns) {
                if (column.getConstraint() == Constraint.PRIMARY_KEY) {
                    primaryColumn = column;
                    break;
                }
            }

            for (String line : fileLines) {
                String[] columnList = line.split(ApplicationConfiguration.DELIMITER_REGEX);
                if (primaryColumn != null) {
                    int primaryColumnIndex = primaryColumn.getColumnPosition();
                    String primaryKeyValue = columnList[primaryColumnIndex];
                    uniqueIds.add(primaryKeyValue);
                }
            }

            if (uniqueIds.isEmpty() || !ConstraintCheck.checkForeignKeyConstraints(tableInfo.getTableName(), uniqueIds, databaseSite)) {
                if (isAutoCommit) {
                    databaseSite.deleteOnlyTable(tableInfo.getTableName());
                    //No Constraints
                    List<String> columnNames = columns.stream().map(Column::getColumnName).collect(Collectors.toList());
                    databaseSite.writeData(tableInfo.getTableName(), columnNames);
                } else {
                    FileWriter.writeTransactionFile(query);
                }

                int deletedRows = fileLines.size();
                databaseSite.decrementRowCountInLocalDataDictionary(tableInfo.getTableName(), deletedRows);
                LOGGER.info("{} rows delete successfully!", deletedRows);

                EventLogger.log(deletedRows + " rows delete successfully from table '" + tableInfo.getTableName() + "'");
            }
        } catch (Exception e) {
            LOGGER.error("Error occurred while deleting the table");
            EventLogger.error(e.getMessage());
        }
        return true;
    }


    public String[] getWhereParameters(String query) {
        String[] splitByWhere = query.split("WHERE");
        return splitByWhere[1].trim().split(" ");
    }

    public boolean deleteTableWhere(TableInfo tableInfo, String query, DatabaseSite databaseSite, boolean isAutoCommit) {
        String[] whereParameters = getWhereParameters(query);
        List<String> fileLines;
        List<String> uniqueIds = new ArrayList<>();
        List<String> lineIds = new ArrayList<>();

        try {
            fileLines = databaseSite.readData(tableInfo.getTableName());
            List<String> headers = Arrays.asList(fileLines.get(0).split(ApplicationConfiguration.DELIMITER_REGEX));
            int headerIndex = headers.indexOf(whereParameters[0]);
            fileLines.remove(0);
            if (ConstraintCheck.checkQueryConstraints(tableInfo.getTableName(), whereParameters[0], whereParameters[2].replace(";", ""), databaseSite)) {

                List<Column> columns = databaseSite.readMetadata(tableInfo.getTableName());

                Column primaryColumn = null;
                for (Column column : columns) {
                    if (column.getConstraint() == Constraint.PRIMARY_KEY) {
                        primaryColumn = column;
                        break;
                    }
                }

                String whereValue = whereParameters[2].replace(";", "");
                whereValue = whereValue.replace("'", "");

                for (String line : fileLines) {
                    String[] columnList = line.split(ApplicationConfiguration.DELIMITER_REGEX);
                    if (columnList[headerIndex].equalsIgnoreCase(whereValue)) {
                        if (primaryColumn != null) {
                            int primaryColumnIndex = primaryColumn.getColumnPosition();
                            String primaryKeyValue = columnList[primaryColumnIndex];
                            uniqueIds.add(primaryKeyValue);
                        }
                    } else {
                        lineIds.add(line);
                    }
                }

                if (uniqueIds.isEmpty() || !ConstraintCheck.checkForeignKeyConstraints(tableInfo.getTableName(), uniqueIds, databaseSite)) {
                    if (isAutoCommit) {
                        databaseSite.deleteOnlyTable(tableInfo.getTableName());
                        databaseSite.writeData(tableInfo.getTableName(), databaseSite.readColumnMetadata(tableInfo.getTableName()));
                        //No Constraints
                        for (String line : lineIds) {
                            databaseSite.writeData(tableInfo.getTableName(), Arrays.asList(line.split(ApplicationConfiguration.DELIMITER_REGEX)));
                        }
                    } else {
                        FileWriter.writeTransactionFile(query);
                    }

                    int deletedRows = fileLines.size() - lineIds.size();
                    databaseSite.decrementRowCountInLocalDataDictionary(tableInfo.getTableName(), deletedRows);
                    LOGGER.info("{} rows delete successfully!", deletedRows);

                    EventLogger.log(deletedRows + " rows delete successfully from table '" + tableInfo.getTableName() + "'");
                }
            }
        } catch (Exception e) {
            LOGGER.error("Error occurred while deleting the table");
            EventLogger.error(e.getMessage());
        }
        return true;
    }
}