package com.group21.server.queries.update;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.logging.log4j.util.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.group21.configurations.ApplicationConfiguration;
import com.group21.server.logger.EventLogger;
import com.group21.server.models.Column;
import com.group21.server.models.DatabaseSite;
import com.group21.server.models.TableInfo;
import com.group21.server.queries.constraints.ConstraintCheck;
import com.group21.utils.FileReader;
import com.group21.utils.FileWriter;
import com.group21.utils.RegexUtil;

public class UpdateParser {
    private static final Logger LOGGER = LoggerFactory.getLogger(UpdateParser.class);
    private static final String UPDATE_TABLE_REGEX = "UPDATE [a-zA-Z_]+ SET [a-zA-Z_]+ (=) [a-zA-Z0-9_']+;?";
    private static final String UPDATE_TABLE_WHERE_REGEX = "UPDATE [a-zA-Z_]+ SET [a-zA-Z_]+ (=) [a-zA-Z0-9_']+ WHERE [a-zA-Z_]+ (=) '?[a-zA-Z0-9_]+'?;?";

    public boolean isValid(String query) {
        String matchedQuery = RegexUtil.getMatch(query, UPDATE_TABLE_REGEX);
        String matchedWhereQuery = RegexUtil.getMatch(query, UPDATE_TABLE_WHERE_REGEX);
        if (Strings.isBlank(matchedQuery) && Strings.isBlank(matchedWhereQuery)) {
            LOGGER.error("Syntax error in provided update query!");
            return false;
        }
        return true;
    }

    public boolean isWhereConditionExists(String query) {
        return query.matches(UPDATE_TABLE_WHERE_REGEX);
    }

    public String getTableName(String query) {
        int indexOfFirstSpace = query.indexOf(' ');
        int indexOfSecondSpace = query.indexOf(' ', indexOfFirstSpace + 1);

        return query.substring(indexOfFirstSpace + 1, indexOfSecondSpace).trim();
    }

    private String[] getSetParameters(String query) {

        if (query.matches(UPDATE_TABLE_WHERE_REGEX)) {
            String setQuery = "";
            int setStartIndex = query.indexOf("SET") + 4;
            int setEndIndex = query.indexOf("WHERE") - 1;
            setQuery = query.substring(setStartIndex, setEndIndex);
            return setQuery.split(" ");
        }
        String[] splitBySet = query.split("SET");
        return splitBySet[1].trim().split(" ");
    }

    private String[] getWhereParameters(String query) {
        String[] splitByWhere = query.split("WHERE");
        return splitByWhere[1].trim().split(" ");
    }

    public void updateTableWhere(TableInfo tableInfo, String query, DatabaseSite databaseSite, boolean isAutoCommit) {
        Map<String, DatabaseSite> gddMap = FileReader.readDistributedDataDictionary();
        String[] whereParameters = getWhereParameters(query);
        String[] setParameters = getSetParameters(query);
        String newValue = setParameters[2].replace(";", "");
        String whereValue = whereParameters[2].replace(";", "");
        try {
            List<String> fileLines = databaseSite.readData(tableInfo.getTableName());
            List<String> headers = Arrays.asList(fileLines.get(0).split(ApplicationConfiguration.DELIMITER_REGEX));
            int setHeaderIndex = headers.indexOf(setParameters[0]);
            int whereHeaderIndex = headers.indexOf(whereParameters[0]);
            List<String> conditionalFileLines = new ArrayList<>();
            List<String> nonConditionalLines = new ArrayList<>();
            List<String> primaryIds = new ArrayList<>();
            fileLines.remove(0);

            if (ConstraintCheck.checkQueryConstraints(tableInfo.getTableName(), setParameters[0], newValue, databaseSite) &&
                    ConstraintCheck.checkQueryConstraints(tableInfo.getTableName(), whereParameters[0], whereValue, databaseSite)) {

                whereValue = whereValue.replace("'", "");
                List<Column> columns = databaseSite.readMetadata(tableInfo.getTableName());

                List<Column> filteredSetColumns =
                        columns.stream().filter(
                                t -> t.getColumnName().equals(setParameters[0])
                        ).collect(Collectors.toList());

                List<Column> filteredWhereColumns =
                        columns.stream().filter(
                                t -> t.getColumnName().equals(whereParameters[0])
                        ).collect(Collectors.toList());

                if (filteredSetColumns.get(0).getConstraint().getKeyword().equals("PRIMARY KEY")) {
                    int primaryCount = 0;
                    for (String line : fileLines) {
                        primaryIds.add(line.split(ApplicationConfiguration.DELIMITER_REGEX)[setHeaderIndex]);
                        String whereIndexValue = line.split(ApplicationConfiguration.DELIMITER_REGEX)[whereHeaderIndex];
                        if (whereIndexValue.equalsIgnoreCase(whereValue)) {
                            primaryCount++;
                        }
                    }

                    if (primaryCount > 1) {
                        LOGGER.info("Update query can not be executed on primary key as where condition matches multiple rows!");
                        return;
                    }

                    if (primaryIds.contains(newValue)) {
                        LOGGER.error("Primary Key constraint violated! Duplicate primary key");
                        return;
                    }

                    if (filteredWhereColumns.get(0).getColumnName().equalsIgnoreCase(filteredSetColumns.get(0).getColumnName())) {
                        if (ConstraintCheck.checkForeignKeyConstraints(tableInfo.getTableName(), Collections.singletonList(whereValue), databaseSite)) {
                            return;
                        }
                    }
                }

                if (filteredSetColumns.get(0).getConstraint().getKeyword().equals("FOREIGN KEY")) {
                    String foreignKeyTableName = filteredSetColumns.get(0).getForeignKeyTable();
                    DatabaseSite foreignKeyTableDatabaseSite = gddMap.get(foreignKeyTableName);

                    if (ApplicationConfiguration.CURRENT_SITE == DatabaseSite.REMOTE) {
                        if (foreignKeyTableDatabaseSite == DatabaseSite.REMOTE) {
                            foreignKeyTableDatabaseSite = DatabaseSite.LOCAL;
                            if (ConstraintCheck.checkPrimaryKeyConstraints(filteredSetColumns.get(0).getForeignKeyTable(), newValue, foreignKeyTableDatabaseSite)) {
                                LOGGER.error("Foreign Key constraint violated! Foreign Key " + newValue + " Does not exist in " + filteredSetColumns.get(0).getForeignKeyTable());
                                return;
                            }
                        }
                    } else {
                        if (ConstraintCheck.checkPrimaryKeyConstraints(filteredSetColumns.get(0).getForeignKeyTable(), newValue, foreignKeyTableDatabaseSite)) {
                            LOGGER.error("Foreign Key constraint violated! Foreign Key " + newValue + " Does not exist in " + filteredSetColumns.get(0).getForeignKeyTable());
                            return;
                        }
                    }
                }

                for (String line : fileLines) {
                    String[] columnList = line.split(ApplicationConfiguration.DELIMITER_REGEX);
                    if (columnList[whereHeaderIndex].equalsIgnoreCase(whereValue)) {
                        //For replacement
                        newValue = newValue.replace("'", "");

                        columnList[setHeaderIndex] = newValue;
                        conditionalFileLines.add(String.join(ApplicationConfiguration.DELIMITER, columnList));
                    } else {
                        nonConditionalLines.add(String.join(ApplicationConfiguration.DELIMITER, columnList));
                    }
                }
                int changedRows = conditionalFileLines.size();

                conditionalFileLines.addAll(nonConditionalLines);
                if (isAutoCommit) {
                    List<String> columnNames = columns.stream().map(Column::getColumnName).collect(Collectors.toList());
                    databaseSite.deleteOnlyTable(tableInfo.getTableName());
                    databaseSite.writeData(tableInfo.getTableName(), columnNames);

                    for (String line : conditionalFileLines) {
                        databaseSite.writeData(tableInfo.getTableName(), Arrays.asList(line.split(ApplicationConfiguration.DELIMITER_REGEX)));
                    }
                } else {
                    FileWriter.writeTransactionFile(query);
                }

                LOGGER.info("{} rows updated successfully!", changedRows);

                EventLogger.log(changedRows + " rows updated successfully in table '" + tableInfo.getTableName() + "'");
            }
        } catch (Exception exception) {
            LOGGER.info("Error occurred while updating the table!");
            EventLogger.error(exception.getMessage());
        }

    }

    public void updateTable(TableInfo tableInfo, String query, DatabaseSite databaseSite, boolean isAutoCommit) {
        String[] setParameters = getSetParameters(query);
        String newValue = setParameters[2].replace(";", "");
        try {
            List<String> fileLines = databaseSite.readData(tableInfo.getTableName());
            List<String> headers = Arrays.asList(fileLines.get(0).split(ApplicationConfiguration.DELIMITER_REGEX));
            int headerIndex = headers.indexOf(setParameters[0]);
            fileLines.remove(0);
            List<String> writeFileLines = new ArrayList<>();

            if (ConstraintCheck.checkQueryConstraints(tableInfo.getTableName(), setParameters[0], newValue, databaseSite)) {
                List<Column> columns = databaseSite.readMetadata(tableInfo.getTableName());
                List<Column> filteredColumns =
                        columns.stream().filter(
                                t -> t.getColumnName().equals(setParameters[0])
                        ).collect(Collectors.toList());

                if (filteredColumns.get(0).getConstraint().getKeyword().equals("PRIMARY KEY")) {
                    LOGGER.error("Primary Key constraint violated! Duplicate primary key");
                    return;
                }
                if (filteredColumns.get(0).getConstraint().getKeyword().equals("FOREIGN KEY")) {
                    Map<String, DatabaseSite> gddMap = FileReader.readDistributedDataDictionary();
                    String foreignKeyTableName = filteredColumns.get(0).getForeignKeyTable();
                    DatabaseSite foreignKeyTableDatabaseSite = gddMap.get(foreignKeyTableName);
                    if (ApplicationConfiguration.CURRENT_SITE == DatabaseSite.REMOTE) {
                        if (foreignKeyTableDatabaseSite == DatabaseSite.REMOTE) {
                            foreignKeyTableDatabaseSite = DatabaseSite.LOCAL;
                            if (ConstraintCheck.checkPrimaryKeyConstraints(foreignKeyTableName, newValue, foreignKeyTableDatabaseSite)) {
                                LOGGER.error("Foreign Key constraint violated! Foreign Key " + newValue + " Does not exist in " + filteredColumns.get(0).getForeignKeyTable());
                                return;
                            }
                        }
                    } else {
                        if (ConstraintCheck.checkPrimaryKeyConstraints(foreignKeyTableName, newValue, foreignKeyTableDatabaseSite)) {
                            LOGGER.error("Foreign Key constraint violated! Foreign Key " + newValue + " Does not exist in " + filteredColumns.get(0).getForeignKeyTable());
                            return;
                        }
                    }
                }

                if (isAutoCommit) {
                    for (String line : fileLines) {
                        String[] columnList = line.split(ApplicationConfiguration.DELIMITER_REGEX);
                        //For replacement
                        newValue = newValue.replace("'", "");

                        columnList[headerIndex] = newValue;
                        writeFileLines.add(String.join(ApplicationConfiguration.DELIMITER, columnList));
                    }

                    List<String> columnNames = columns.stream().map(Column::getColumnName).collect(Collectors.toList());
                    databaseSite.deleteOnlyTable(tableInfo.getTableName());
                    databaseSite.writeData(tableInfo.getTableName(), columnNames);

                    for (String line : writeFileLines) {
                        databaseSite.writeData(tableInfo.getTableName(), Arrays.asList(line.split(ApplicationConfiguration.DELIMITER_REGEX)));
                    }
                } else {
                    FileWriter.writeTransactionFile(query);
                }

                int changedRows = fileLines.size();
                LOGGER.info("{} rows updated successfully!", changedRows);

                EventLogger.log(changedRows + " rows updated successfully in table '" + tableInfo.getTableName() + "'");
            }
        } catch (Exception exception) {
            LOGGER.info("Error occurred while updating the table!");
            EventLogger.error(exception.getMessage());
        }
    }
}

