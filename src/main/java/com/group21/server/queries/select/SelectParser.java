package com.group21.server.queries.select;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.util.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.group21.configurations.ApplicationConfiguration;
import com.group21.constants.CommonRegex;
import com.group21.server.models.Column;
import com.group21.server.models.DataType;
import com.group21.server.models.DatabaseSite;
import com.group21.utils.FileReader;
import com.group21.utils.RegexUtil;

public class SelectParser {

    private static final Logger LOGGER = LoggerFactory.getLogger(SelectParser.class);

    private static final String SELECT_REGEX_TYPE1 = "^SELECT \\* FROM [a-zA-Z_]+;?$";
    private static final String SELECT_REGEX_TYPE2 = "^SELECT [a-zA-Z_, ]+ FROM [a-zA-Z_]+;?$";
    private static final String SELECT_REGEX_TYPE3 = "^SELECT \\* FROM [a-zA-Z_]+ WHERE [a-zA-Z_]+ = .+;?$";
    private static final String SELECT_REGEX_TYPE4 = "^SELECT [a-zA-Z_, ]+ FROM [a-zA-Z_]+ WHERE [a-zA-Z_]+ = .+;?$";

    public boolean isValid(String query) {
        int queryType = getQueryType(query);

        if (queryType == 0) {
            LOGGER.error("Syntax error in provided insert into table query.");
            return false;
        }

        String tableName = getTableName(query);

        DatabaseSite databaseSite = getDatabaseSite(tableName);

        if (databaseSite == null) {
            return false;
        }

        List<Column> columnData = databaseSite.readMetadata(tableName);
        List<String> columnNameList = new ArrayList<>();
        for (Column c : columnData) {
            columnNameList.add(c.getColumnName());
        }

        if (queryType == 2 || queryType == 4) {
            List<String> columnList = getColumns(query, databaseSite);

            for (String s : columnList) {
                if (!columnNameList.contains(s)) {
                    LOGGER.error("Column '{}' does not exist in table '{}' ", s, tableName);
                    return false;
                }
            }
        }

        if (queryType == 3 || queryType == 4) {
            String conditionParameter = getConditionParameter(query);
            String conditionValue = getConditionValue(query);
            if (!columnNameList.contains(conditionParameter)) {
                LOGGER.error("Column '{}' does not exist in table '{}' ", conditionParameter, tableName);
                return false;
            }

            DataType conditionType = columnData.get(columnNameList.indexOf(conditionParameter)).getColumnType();

            if (conditionType.equals(DataType.INT)) {
                try {
                    Integer.parseInt(conditionValue);
                } catch (Exception e) {
                    conditionValue = "";
                }
            } else if (conditionType.equals(DataType.DOUBLE)) {
                try {
                    Double.parseDouble(conditionValue);
                } catch (Exception e) {
                    conditionValue = "";
                }
            } else if (conditionType.equals(DataType.TEXT)) {
                conditionValue = RegexUtil.getMatch(conditionValue, CommonRegex.TEXT_REGEX);

                if (conditionValue != null) {
                    conditionValue = conditionValue.substring(1, conditionValue.length() - 1);
                }
            }

            if (Strings.isBlank(conditionValue)) {
                LOGGER.error("Column '{}' requires value of type '{}'", conditionParameter, conditionType.name());
                return false;
            }

        }

        return true;
    }


    public String getTableName(String query) {
        int tableNameStartIndex = query.indexOf("FROM") + 5;
        int tableNameEndIndex = query.indexOf(" ", tableNameStartIndex);
        if (tableNameEndIndex == -1) {
            tableNameEndIndex = query.indexOf(";");
            if (tableNameEndIndex == -1) {
                tableNameEndIndex = query.length();
            }
        }
        return query.substring(tableNameStartIndex, tableNameEndIndex).trim();
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

    public List<String> getColumns(String query, DatabaseSite databaseSite) {
        int columnStartIndex = query.indexOf("SELECT") + 7;
        int columnEndIndex = query.indexOf("FROM", columnStartIndex);
        String column = query.substring(columnStartIndex, columnEndIndex).trim();
        List<String> columnList = new ArrayList<>();
        if (column.equals("*")) {
            String tableName = getTableName(query);
            List<Column> columnDataList = databaseSite.readMetadata(tableName);
            for (Column c : columnDataList) {
                columnList.add(c.getColumnName());
            }
        } else {
            String[] columnArray = column.split(",");
            for (String s : columnArray) {
                columnList.add(s.trim());
            }
        }
        return columnList;
    }

    public String getConditionParameter(String query) {
        int parameterStartIndex = query.indexOf("WHERE") + 6;
        int parameterEndIndex = query.indexOf(" ", parameterStartIndex);
        return query.substring(parameterStartIndex, parameterEndIndex).trim();
    }

    public String getConditionValue(String query) {
        int parameterStartIndex = query.indexOf("=") + 2;
        int parameterEndIndex = query.indexOf(";", parameterStartIndex);
        if (parameterEndIndex == -1) {
            parameterEndIndex = query.length();
        }
        return query.substring(parameterStartIndex, parameterEndIndex).trim();
    }

    public int getQueryType(String query) {
        String matchedQueryType1 = RegexUtil.getMatch(query, SELECT_REGEX_TYPE1);
        String matchedQueryType2 = RegexUtil.getMatch(query, SELECT_REGEX_TYPE2);
        String matchedQueryType3 = RegexUtil.getMatch(query, SELECT_REGEX_TYPE3);
        String matchedQueryType4 = RegexUtil.getMatch(query, SELECT_REGEX_TYPE4);
        if (Strings.isBlank(matchedQueryType1) && Strings.isBlank(matchedQueryType2) && Strings.isBlank(matchedQueryType3) && Strings.isBlank(matchedQueryType4)) {
            return 0;
        } else if (Strings.isBlank(matchedQueryType2) && Strings.isBlank(matchedQueryType3) && Strings.isBlank(matchedQueryType4)) {
            return 1;
        } else if (Strings.isBlank(matchedQueryType3) && Strings.isBlank(matchedQueryType4)) {
            return 2;
        } else if (Strings.isBlank(matchedQueryType4)) {
            return 3;
        } else {
            return 4;
        }
    }

}
