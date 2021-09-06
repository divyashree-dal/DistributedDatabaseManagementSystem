package com.group21.server.queries.insert;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.group21.server.logger.EventLogger;
import com.group21.server.models.DatabaseSite;
import com.group21.utils.FileWriter;


public class InsertQueryExecutor {

    private static final Logger LOGGER = LoggerFactory.getLogger(InsertQueryExecutor.class);

    private final InsertParser insertParser;

    public InsertQueryExecutor() {
        this.insertParser = new InsertParser();
    }

    public void execute(String query, boolean isAutoCommit) {
        boolean isQueryValid = insertParser.isValid(query);

        if (isQueryValid) {
            String tableName = insertParser.getTableName(query);
            DatabaseSite databaseSite = insertParser.getDatabaseSite(tableName);
            List<String> columnValues = insertParser.getColumnValues(query, tableName, databaseSite);

            if (isAutoCommit) {
                databaseSite.writeData(tableName, columnValues);
                databaseSite.incrementRowCountInLocalDataDictionary(tableName);
            } else {
                FileWriter.writeTransactionFile(query);
            }

            LOGGER.info("1 row inserted successfully!");

            EventLogger.log("1 row inserted successfully in table '" + tableName + "'");
        }
    }
}