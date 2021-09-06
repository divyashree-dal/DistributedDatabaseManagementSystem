package com.group21.server.queries.createtable;

import java.util.LinkedList;
import java.util.List;

import org.apache.logging.log4j.util.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.group21.configurations.ApplicationConfiguration;
import com.group21.server.models.Column;
import com.group21.server.models.Constraint;
import com.group21.server.models.DataType;
import com.group21.utils.RegexUtil;

public class CreateTableParser {

    private static final Logger LOGGER = LoggerFactory.getLogger(CreateTableParser.class);

    private static final String CREATE_TABLE_REGEX = "^CREATE TABLE [a-zA-Z_]* (NODE (LOCAL|REMOTE) )?\\(.*\\);?$";
    private static final String VALID_COLUMN_SYNTAX_REGEX = "^[a-zA-Z_]* %s$";

    public boolean isValid(String query) {
        String matchedQuery = RegexUtil.getMatch(query, CREATE_TABLE_REGEX);

        if (Strings.isBlank(matchedQuery)) {
            LOGGER.error("Syntax error in provided create table query.");
            return false;
        }

        int firstBracketIndex = query.indexOf('(');
        int lastBracketIndex = query.lastIndexOf(')');

        String columnString = query.substring(firstBracketIndex + 1, lastBracketIndex);

        if (Strings.isBlank(columnString)) {
            LOGGER.error("Please provide columns to create a table.");
            return false;
        }

        String[] columnArray = columnString.split(",");
        for (String columnData : columnArray) {
            columnData = columnData.trim();

            if (columnData.endsWith(Constraint.PRIMARY_KEY.getKeyword())) {
                if (!Constraint.validPrimaryKeySyntax(columnData)) {
                    LOGGER.error("Column details - '{}' doesn't have valid primary key syntax.", columnData);
                    return false;
                }
            } else if (columnData.contains(Constraint.FOREIGN_KEY.getKeyword())) {
                if (!Constraint.validForeignKeySyntax(columnData)) {
                    LOGGER.error("Column details - '{}' doesn't have valid foreign key syntax.", columnData);
                    return false;
                }
            } else {
                String allowedTypesRegex = DataType.getAllowedTypesRegex();
                String matchedColumnData = RegexUtil.getMatch(columnData, String.format(VALID_COLUMN_SYNTAX_REGEX, allowedTypesRegex));
                if (Strings.isBlank(matchedColumnData)) {
                    LOGGER.error("Column details - '{}' has syntax error.", columnData);
                    return false;
                }
            }
        }

        return true;
    }

    public String getTableName(String query) {
        int indexOfFirstSpace = query.indexOf(' ');
        int indexOfSecondSpace = query.indexOf(' ', indexOfFirstSpace + 1);
        int indexOfThirdSpace = query.indexOf(' ', indexOfSecondSpace + 1);

        return query.substring(indexOfSecondSpace + 1, indexOfThirdSpace).trim();
    }

    public String getDatabaseSite(String query) {
        int indexOfNode = query.indexOf("NODE");
        if (indexOfNode == -1) {
            return ApplicationConfiguration.CURRENT_SITE.name();
        }

        int indexOfSpaceAfterNode = query.indexOf(' ', indexOfNode + 1);
        int indexOfSecondSpaceAfterNode = query.indexOf(' ', indexOfSpaceAfterNode + 1);

        return query.substring(indexOfSpaceAfterNode + 1, indexOfSecondSpaceAfterNode).trim();
    }

    public List<Column> getColumns(String query) {
        List<Column> columns = new LinkedList<>();

        int firstBracketIndex = query.indexOf('(');
        int lastBracketIndex = query.lastIndexOf(')');

        String columnString = query.substring(firstBracketIndex + 1, lastBracketIndex);

        String[] columnArray = columnString.split(",");
        for (String columnData : columnArray) {
            columnData = columnData.trim();

            Column column = new Column();

            int indexOfFirstSpace = columnData.indexOf(' ');
            if (columnData.endsWith(Constraint.PRIMARY_KEY.getKeyword())) {
                int indexOfSecondSpace = columnData.indexOf(' ', indexOfFirstSpace + 1);

                String columnName = columnData.substring(0, indexOfFirstSpace);
                DataType columnType = DataType.from(columnData.substring(indexOfFirstSpace + 1, indexOfSecondSpace));

                column.setColumnName(columnName);
                column.setColumnType(columnType);
                column.setConstraint(Constraint.PRIMARY_KEY);
            } else if (columnData.contains(Constraint.FOREIGN_KEY.getKeyword())) {
                int indexOfSecondSpace = columnData.indexOf(' ', indexOfFirstSpace + 1);

                String columnName = columnData.substring(0, indexOfFirstSpace);
                DataType columnType = DataType.from(columnData.substring(indexOfFirstSpace + 1, indexOfSecondSpace));

                int referencesIndex = columnData.indexOf("REFERENCES");
                int spaceIndexAfterReferences = columnData.indexOf(' ', referencesIndex + 1);
                int openBracketIndex = columnData.indexOf('(', referencesIndex + 1);
                int closeBracketIndex = columnData.indexOf(')', referencesIndex + 1);

                String foreignKeyTableName = columnData.substring(spaceIndexAfterReferences + 1, openBracketIndex).trim();
                String foreignKeyColumnName = columnData.substring(openBracketIndex + 1, closeBracketIndex).trim();

                column.setColumnName(columnName);
                column.setColumnType(columnType);
                column.setConstraint(Constraint.FOREIGN_KEY);
                column.setForeignKeyTable(foreignKeyTableName);
                column.setForeignKeyColumnName(foreignKeyColumnName);
            } else {
                String columnName = columnData.substring(0, indexOfFirstSpace);
                DataType columnType = DataType.from(columnData.substring(indexOfFirstSpace + 1));

                column.setColumnName(columnName);
                column.setColumnType(columnType);
                column.setConstraint(Constraint.UNKNOWN);
            }

            columns.add(column);
        }

        return columns;
    }
}
