package com.group21.server.queries.select;

import java.util.ArrayList;
import java.util.List;

import com.group21.server.models.Column;
import com.group21.server.models.DatabaseSite;
import com.group21.server.models.TableInfo;

public class SelectQueryExecutor {

    private final SelectParser selectParser;

    public SelectQueryExecutor() {
        this.selectParser = new SelectParser();
    }

    public void execute(String query) {
        boolean isQueryValid = selectParser.isValid(query);

        if (isQueryValid) {
            int queryType = selectParser.getQueryType(query);
            String tableName = selectParser.getTableName(query);
            DatabaseSite databaseSite = selectParser.getDatabaseSite(tableName);
            List<String> columnNames = selectParser.getColumns(query, databaseSite);

            List<List<String>> selectData = new ArrayList<>();
            selectData.add(columnNames);

            List<TableInfo> tableInfoList = databaseSite.readLocalDataDictionary();
            List<String> tableNameList = new ArrayList<>();
            for (TableInfo tableInfo : tableInfoList) {
                tableNameList.add(tableInfo.getTableName());
            }
            int tableRowCount = tableInfoList.get(tableNameList.indexOf(tableName)).getNumberOfRows();

            if (queryType == 1 || queryType == 2) {

                List<List<String>> columnDataList = new ArrayList<>();
                for (String columnName : columnNames) {
                    List<String> columnData = databaseSite.readColumnData(tableName, columnName);
                    columnDataList.add(columnData);
                }

                for (int i = 0; i < tableRowCount; i++) {
                    List<String> rowData = new ArrayList<>();
                    for (int j = 0; j < columnDataList.size(); j++) {
                        rowData.add(columnDataList.get(j).get(i));
                    }
                    selectData.add(rowData);
                }
            } else {
                List<List<String>> columnDataList = new ArrayList<>();
                List<Column> allColumnMetadataData = databaseSite.readMetadata(tableName);
                List<String> allColumnNames = new ArrayList<>();

                for (Column c : allColumnMetadataData) {
                    allColumnNames.add(c.getColumnName());
                }

                for (String columnName : allColumnNames) {
                    List<String> columnData = databaseSite.readColumnData(tableName, columnName);
                    columnDataList.add(columnData);
                }

                String conditionParameter = selectParser.getConditionParameter(query);
                String conditionValue = selectParser.getConditionValue(query).replace("'", "");
                int conditionParameterIndex = allColumnNames.indexOf(conditionParameter);

                for (int i = 0; i < tableRowCount; i++) {
                    if (columnDataList.get(conditionParameterIndex).get(i).equals(conditionValue)) {
                        List<String> rowData = new ArrayList<>();
                        for (String columnName : columnNames) {
                            int columnNameIndex = allColumnNames.indexOf(columnName);
                            rowData.add(columnDataList.get(columnNameIndex).get(i));
                        }
                        selectData.add(rowData);
                    }
                }
            }

            System.out.println();
            for (int i = 0; i < selectData.size(); i++) {
                for (int j = 0; j < selectData.get(i).size(); j++) {
                    if (j == 0 && i > 0) {
                        System.out.println();
                    } else if (j > 0) {
                        System.out.print("|");
                    }
                    System.out.print(selectData.get(i).get(j));
                }
            }
            System.out.println();
            System.out.println("\n" + (selectData.size() - 1) + " rows returned");
        }
    }
}
