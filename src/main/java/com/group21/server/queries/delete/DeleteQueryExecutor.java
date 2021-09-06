package com.group21.server.queries.delete;

import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.group21.configurations.ApplicationConfiguration;
import com.group21.server.models.DatabaseSite;
import com.group21.server.models.TableInfo;
import com.group21.utils.FileReader;

public class DeleteQueryExecutor {

    private static final Logger LOGGER = LoggerFactory.getLogger(DeleteQueryExecutor.class);

    private final DeleteParser deleteQueryParser;

    public DeleteQueryExecutor() {
        this.deleteQueryParser = new DeleteParser();
    }

    public void execute(String query, boolean isAutoCommit) {
        boolean isValid = deleteQueryParser.isValid(query);
        if (isValid) {
            String tableName = deleteQueryParser.getTableName(query);

            Map<String, DatabaseSite> gddMap = FileReader.readDistributedDataDictionary();

            if (gddMap.containsKey(tableName)) {
                DatabaseSite databaseSite = gddMap.get(tableName);

                DatabaseSite databaseOperationSite = DatabaseSite.LOCAL;
                if (databaseSite != ApplicationConfiguration.CURRENT_SITE) {
                    databaseOperationSite = DatabaseSite.REMOTE;
                }

                if (databaseOperationSite == DatabaseSite.REMOTE && ApplicationConfiguration.CURRENT_SITE == DatabaseSite.REMOTE) {
                    LOGGER.error("Table '{}' is on LOCAL site & Remote server can not connect to local machine.", tableName);
                    return;
                }

                List<TableInfo> tableInfoList = databaseOperationSite.readLocalDataDictionary();

                for (TableInfo tableInfo : tableInfoList) {
                    if (tableInfo.getTableName().equals(tableName)) {
                        if (deleteQueryParser.isWhereConditionExists(query)) {
                            deleteQueryParser.deleteTableWhere(tableInfo, query, databaseOperationSite, isAutoCommit);
                        } else {
                            deleteQueryParser.deleteTable(tableInfo, query, databaseOperationSite, isAutoCommit);
                        }
                    }
                }
            } else {
                LOGGER.info("Table '{}' does not exist in database!", tableName);
            }
        }
    }
}
