package com.group21.server.queries.createtable;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.group21.configurations.ApplicationConfiguration;
import com.group21.server.logger.EventLogger;
import com.group21.server.models.Column;
import com.group21.server.models.Constraint;
import com.group21.server.models.DatabaseSite;
import com.group21.server.models.TableInfo;
import com.group21.utils.FileReader;
import com.group21.utils.FileWriter;

public class CreateTableQueryExecutor {

    private static final Logger LOGGER = LoggerFactory.getLogger(CreateTableQueryExecutor.class);

    private final CreateTableParser createTableParser;

    public CreateTableQueryExecutor() {
        this.createTableParser = new CreateTableParser();
    }

    public void execute(String query) {
        boolean isQueryValid = createTableParser.isValid(query);

        if (isQueryValid) {
            String tableName = createTableParser.getTableName(query);
            DatabaseSite databaseSite = DatabaseSite.from(createTableParser.getDatabaseSite(query));

            DatabaseSite databaseOperationSite = DatabaseSite.LOCAL;
            if (databaseSite != ApplicationConfiguration.CURRENT_SITE) {
                databaseOperationSite = DatabaseSite.REMOTE;
            }

            if (databaseOperationSite == DatabaseSite.REMOTE && ApplicationConfiguration.CURRENT_SITE == DatabaseSite.REMOTE) {
                LOGGER.error("Table '{}' is on LOCAL site & Remote server can not connect to local machine.", tableName);
                return;
            }

            List<Column> columns = createTableParser.getColumns(query);

            Map<String, DatabaseSite> tableInfoMap = FileReader.readDistributedDataDictionary();
            Set<String> tableNameList = tableInfoMap.keySet();

            if (tableNameList.contains(tableName)) {
                LOGGER.error("Table '{}' already exists.", tableName);
                return;
            }

            for (Column column : columns) {
                if (column.getConstraint() == Constraint.FOREIGN_KEY && !tableNameList.contains(column.getForeignKeyTable())) {
                    LOGGER.error("Foreign Key Table '{}' does not exist.", column.getForeignKeyTable());
                    return;
                }
            }

            databaseOperationSite.writeMetadata(tableName, columns);

            List<String> tableData = columns.stream().map(Column::getColumnName).collect(Collectors.toList());
            databaseOperationSite.writeData(tableName, tableData);

            TableInfo tableInfo = new TableInfo();
            tableInfo.setTableName(tableName);
            tableInfo.setNumberOfRows(0);
            tableInfo.setCreatedOn(System.currentTimeMillis());

            databaseOperationSite.writeLocalDataDictionary(tableInfo);

            FileWriter.writeDistributedDataDictionary(tableName, databaseSite);

            LOGGER.info("Table '{}' created Successfully.", tableName);

            EventLogger.log("Table '" + tableName + "' created Successfully.");
        }
    }
}
