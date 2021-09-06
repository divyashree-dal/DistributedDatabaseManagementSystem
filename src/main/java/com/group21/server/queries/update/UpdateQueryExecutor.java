package com.group21.server.queries.update;

import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.group21.configurations.ApplicationConfiguration;
import com.group21.server.models.DatabaseSite;
import com.group21.server.models.TableInfo;
import com.group21.utils.FileReader;

public class UpdateQueryExecutor {
    private static final Logger LOGGER = LoggerFactory.getLogger(UpdateQueryExecutor.class);

    private final UpdateParser updateQueryParser;

    public UpdateQueryExecutor() {
        this.updateQueryParser = new UpdateParser();
    }

    public void execute(String query, boolean isAutoCommit) {
        boolean isValid = updateQueryParser.isValid(query);
        if (isValid) {
            String tableName = updateQueryParser.getTableName(query);

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
                        if (updateQueryParser.isWhereConditionExists(query)) {
                            updateQueryParser.updateTableWhere(tableInfo, query, databaseOperationSite, isAutoCommit);
                        } else {
                            updateQueryParser.updateTable(tableInfo, query, databaseOperationSite, isAutoCommit);
                        }
                    }
                }
            } else {
                LOGGER.info("Table '{}' does not exist in database!", tableName);
            }
        }
    }
}

