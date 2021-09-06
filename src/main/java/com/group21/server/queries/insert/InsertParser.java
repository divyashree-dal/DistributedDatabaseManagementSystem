package com.group21.server.queries.insert;

import com.group21.configurations.ApplicationConfiguration;
import com.group21.constants.CommonRegex;
import com.group21.server.models.Column;
import com.group21.server.models.Constraint;
import com.group21.server.models.DataType;
import com.group21.server.models.DatabaseSite;
import com.group21.utils.FileReader;
import com.group21.utils.RegexUtil;
import org.apache.logging.log4j.util.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class InsertParser {

    private static final Logger LOGGER = LoggerFactory.getLogger(InsertParser.class);

    private static final String INSERT_REGEX_TYPE1 = "^INSERT INTO [a-zA-Z_]* \\(.*\\) VALUES \\(.*\\);?$";
    private static final String INSERT_REGEX_TYPE2 = "^INSERT INTO [a-zA-Z_]* VALUES \\(.*\\);?$";

    public boolean isValid(String query) {
        String matchedQueryType1 = RegexUtil.getMatch(query, INSERT_REGEX_TYPE1);
        String matchedQueryType2 = RegexUtil.getMatch(query, INSERT_REGEX_TYPE2);

        if (Strings.isBlank(matchedQueryType1) && Strings.isBlank(matchedQueryType2)) {
            LOGGER.error("Syntax error in provided insert into table query.");
            return false;
        }

        String tableName = getTableName(query);

        DatabaseSite databaseSite = getDatabaseSite(tableName);

        if (databaseSite == null) {
            return false;
        }

        int firstColumnValueBracketIndex = query.indexOf("VALUES") + 7;
        int lastColumnValueBracketIndex = query.indexOf(')', firstColumnValueBracketIndex);

        String columnValueString = query.substring(firstColumnValueBracketIndex + 1, lastColumnValueBracketIndex);

        if (Strings.isBlank(columnValueString)) {
            LOGGER.error("Column Values - 'Missing' in provided insert into table query.");
            return false;
        }

        String[] columnValueArray = columnValueString.split(",");
        String[] columnNameArray;

        List<Column> columnList = databaseSite.readMetadata(tableName);

        if (Strings.isBlank(matchedQueryType2)) {
            int firstColumnNameBracketIndex = query.indexOf('(');
            int lastColumnNameBracketIndex = query.indexOf(')');

            String columnNameString = query.substring(firstColumnNameBracketIndex + 1, lastColumnNameBracketIndex);

            if (Strings.isBlank(columnNameString)) {
                LOGGER.error("Column Names - 'Missing' in provided insert into table query.");
                return false;
            }

            columnNameArray = columnNameString.split(",");

            if (columnNameArray.length != columnValueArray.length) {
                LOGGER.error("Number of columns and values mismatch");
                return false;
            }

            List<String> primaryKeyColumnNames = new ArrayList<>();
            for (Column c : columnList) {
                if (c.getConstraint().equals(Constraint.PRIMARY_KEY)) {
                    primaryKeyColumnNames.add(c.getColumnName());
                }
            }

            for (String p : primaryKeyColumnNames) {
                boolean doesNotContainKey = true;
                for (String columnName : columnNameArray) {
                    if (columnName.trim().equals(p)) {
                        doesNotContainKey = false;
                        break;
                    }
                }
                if (doesNotContainKey) {
                    LOGGER.error("Primary Key Value is Missing");
                    return false;
                }
            }

        } else {

            columnNameArray = new String[columnList.size()];

            for (int i = 0; i < columnList.size(); i++) {
                columnNameArray[i] = columnList.get(i).getColumnName();
            }
        }
        return checkConstraints(tableName, databaseSite, columnNameArray, columnValueArray);
    }

    private boolean checkConstraints(String tableName, DatabaseSite databaseSite, String[] columnNameArray, String[] columnValueArray) {

        int columnLength = columnNameArray.length;

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

        for (int i = 0; i < columnLength; i++) {
            String columnName = columnNameArray[i].trim();
            String columnValue = columnValueArray[i].trim();

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

            Constraint columnValueConstraint = columnConstraintList.get(columnName);
            if (columnValueConstraint.equals(Constraint.PRIMARY_KEY)) {
                List<String> primaryKeyValueList = databaseSite.readColumnData(tableName, columnName);

                if (primaryKeyValueList.contains(columnValue)) {
                    LOGGER.error("Primary Key Constraint Violated");
                    return false;
                }
            } else if (columnValueConstraint.equals(Constraint.FOREIGN_KEY)) {
                String foreignKeyTable = columnForeignKeyTableList.get(columnName);
                String foreignKeyColumnName = columnForeignKeyColumnNameList.get(columnName);

                Map<String, DatabaseSite> gddMap = FileReader.readDistributedDataDictionary();
                DatabaseSite foreignKeyTableDatabaseSite = gddMap.get(foreignKeyTable);

                List<String> foreignKeyValueList = new ArrayList<>();
                if (ApplicationConfiguration.CURRENT_SITE != DatabaseSite.REMOTE) {
                    foreignKeyValueList = foreignKeyTableDatabaseSite.readColumnData(foreignKeyTable, foreignKeyColumnName);
                } else if (foreignKeyTableDatabaseSite != DatabaseSite.LOCAL) {
                    foreignKeyValueList = databaseSite.readColumnData(foreignKeyTable, foreignKeyColumnName);
                }

                if (!foreignKeyValueList.contains(columnValue)) {
                    LOGGER.error("Foreign Key Constraint Violated! Foreign Key '{}' Does not exist in '{}'", columnValue, foreignKeyTable);
                    return false;
                }
            }
        }
        return true;
    }

    public String getTableName(String query) {
        int tableNameStartIndex = query.indexOf("INTO") + 5;
        int tableNameEndIndex = query.indexOf(' ', tableNameStartIndex);
        return query.substring(tableNameStartIndex, tableNameEndIndex);
    }

    public List<String> getColumnValues(String query, String tableName, DatabaseSite databaseSite) {
        String matchedQueryType2 = RegexUtil.getMatch(query, INSERT_REGEX_TYPE2);

        List<String> columnValues = new ArrayList<>();

        int firstColumnValueBracketIndex = query.indexOf("VALUES") + 7;
        int lastColumnValueBracketIndex = query.indexOf(')', firstColumnValueBracketIndex);

        String columnValueString = query.substring(firstColumnValueBracketIndex + 1, lastColumnValueBracketIndex);
        String[] columnValueArray = columnValueString.split(",");

        List<Column> columnList = databaseSite.readMetadata(tableName);
        Map<String, DataType> columnNameType = new LinkedHashMap<>();
        for (Column c : columnList) {
            columnNameType.put(c.getColumnName(), c.getColumnType());
        }

        if (Strings.isBlank(matchedQueryType2)) {
            int firstColumnNameBracketIndex = query.indexOf('(');
            int lastColumnNameBracketIndex = query.indexOf(')');
            String columnNameString = query.substring(firstColumnNameBracketIndex + 1, lastColumnNameBracketIndex);
            String[] columnNameArray = columnNameString.split(",");

            Map<String, String> columnNameValue = new LinkedHashMap<>();
            for (int i = 0; i < columnNameArray.length; i++) {
                columnNameValue.put(columnNameArray[i].trim(), columnValueArray[i].trim());
            }
            for (String name : columnNameType.keySet()) {
                if (columnNameValue.containsKey(name)) {
                    String value = columnNameValue.get(name).trim();
                    if (columnNameType.get(name).equals(DataType.TEXT)) {
                        value = value.substring(1, value.length() - 1);
                    }
                    columnValues.add(value);
                } else {
                    columnValues.add("null");
                }
            }
        } else {
            int i = 0;
            for (String name : columnNameType.keySet()) {
                String value = columnValueArray[i++].trim();
                if (columnNameType.get(name).equals(DataType.TEXT)) {
                    value = value.substring(1, value.length() - 1);
                }
                columnValues.add(value);
            }
        }
        return columnValues;
    }

    public DatabaseSite getDatabaseSite(String tableName) {
        Map<String, DatabaseSite> dataDictionary = FileReader.readDistributedDataDictionary();
        DatabaseSite databaseSite = dataDictionary.get(tableName);

        if (databaseSite != null) {
            DatabaseSite databaseOperationSite = DatabaseSite.LOCAL;
            if (databaseSite != ApplicationConfiguration.CURRENT_SITE) {
                databaseOperationSite = DatabaseSite.REMOTE;
            }

            if (databaseOperationSite == DatabaseSite.REMOTE && ApplicationConfiguration.CURRENT_SITE == DatabaseSite.REMOTE) {
                LOGGER.error("Table '{}' is on LOCAL site & Remote server can not connect to local machine.", tableName);
                return null;
            }
            return databaseOperationSite;
        } else {
            LOGGER.error("Table Name '{}' Does not exist ", tableName);
            return null;
        }
    }
}
