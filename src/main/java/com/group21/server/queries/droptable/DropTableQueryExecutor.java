package com.group21.server.queries.droptable;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.group21.configurations.ApplicationConfiguration;
import com.group21.server.logger.EventLogger;
import com.group21.server.models.DatabaseSite;
import com.group21.server.models.TableInfo;
import com.group21.server.queries.constraints.ConstraintCheck;
import com.group21.utils.FileReader;
import com.group21.utils.FileWriter;

public class DropTableQueryExecutor {

    private static final Logger LOGGER = LoggerFactory.getLogger(DropTableQueryExecutor.class);

    private final DropTableParser dropTableParser;

    public DropTableQueryExecutor() {
        this.dropTableParser = new DropTableParser();
    }

    public void execute(String query) {
        boolean isQueryValid = dropTableParser.isValid(query);

        if (isQueryValid) {
            String tableName = dropTableParser.getTableName(query);

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

                if (ConstraintCheck.checkForeignKeyTable(tableName)) {
                    return;
                }

                databaseOperationSite.deleteTable(tableName);

                updateLocalDataDictionary(tableName, databaseOperationSite);

                gddMap.remove(tableName);
                String updatedDistributedDictionaryContent = FileWriter.generateDistributedDataDictionaryContent(gddMap);
                FileWriter.writeFile(ApplicationConfiguration.DISTRIBUTED_DATA_DICTIONARY_NAME, updatedDistributedDictionaryContent);

                LOGGER.info("Table '{}' deleted Successfully.", tableName);

                EventLogger.log("Table '" + tableName + "' deleted Successfully.");
            } else {
                LOGGER.info("Table '{}' does not exist in database!", tableName);
            }
        }
    }

    private void updateLocalDataDictionary(String tableName, DatabaseSite databaseSite) {
        List<TableInfo> existingTableInfoList = databaseSite.readLocalDataDictionary();

        List<TableInfo> newTableInfoList = new ArrayList<>();

        for (TableInfo tableInfo : existingTableInfoList) {
            if (!tableInfo.getTableName().equalsIgnoreCase(tableName)) {
                newTableInfoList.add(tableInfo);
            }
        }

        String updatedLocalDictionaryContent = FileWriter.generateLocalDataDictionaryContent(newTableInfoList);

        databaseSite.writeFile(ApplicationConfiguration.LOCAL_DATA_DICTIONARY_NAME, updatedLocalDictionaryContent);
    }
}
