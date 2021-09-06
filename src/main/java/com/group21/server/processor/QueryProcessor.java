package com.group21.server.processor;

import java.util.Date;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.group21.configurations.ApplicationConfiguration;
import com.group21.server.logger.EventLogger;
import com.group21.server.logger.GeneralLogger;
import com.group21.server.models.DatabaseSite;
import com.group21.server.models.QueryType;
import com.group21.server.queries.createtable.CreateTableQueryExecutor;
import com.group21.server.queries.delete.DeleteQueryExecutor;
import com.group21.server.queries.droptable.DropTableQueryExecutor;
import com.group21.server.queries.insert.InsertQueryExecutor;
import com.group21.server.queries.select.SelectQueryExecutor;
import com.group21.server.queries.truncatetable.TruncateTableQueryExecutor;
import com.group21.server.queries.update.UpdateQueryExecutor;
import com.group21.utils.RemoteDatabaseReader;

public class QueryProcessor {

    private static final Logger LOGGER = LoggerFactory.getLogger(QueryProcessor.class);

    private QueryProcessor() {
    }

    public static void process(String query, boolean isAutoCommit) {
        query = query.toUpperCase();

        if (ApplicationConfiguration.CURRENT_SITE == DatabaseSite.LOCAL) {
            // This is done only for local as remote site can not access local machine
            RemoteDatabaseReader.syncDistributedDataDictionary();
        }

        long startTime = System.currentTimeMillis();
        EventLogger.log("Execution started for query - '" + query + "'");
        LOGGER.debug("Execution started for query - '{}' on {}", query, new Date(startTime));
        QueryType queryType = QueryType.from(query);

        switch (queryType) {
            case CREATE:
                CreateTableQueryExecutor executor = new CreateTableQueryExecutor();
                executor.execute(query);
                break;
            case UPDATE:
                UpdateQueryExecutor updateQueryExecutor = new UpdateQueryExecutor();
                updateQueryExecutor.execute(query, isAutoCommit);
                break;
            case DELETE:
                DeleteQueryExecutor deleteQueryExecutor = new DeleteQueryExecutor();
                deleteQueryExecutor.execute(query, isAutoCommit);
                break;
            case INSERT:
                InsertQueryExecutor insertQueryExecutor = new InsertQueryExecutor();
                insertQueryExecutor.execute(query, isAutoCommit);
                break;
            case SELECT:
                SelectQueryExecutor selectQueryExecutor = new SelectQueryExecutor();
                selectQueryExecutor.execute(query);
                break;
            case DROP:
                DropTableQueryExecutor dropTableQueryExecutor = new DropTableQueryExecutor();
                dropTableQueryExecutor.execute(query);
                break;
            case TRUNCATE:
                TruncateTableQueryExecutor truncateTableQueryExecutor = new TruncateTableQueryExecutor();
                truncateTableQueryExecutor.execute(query);
                break;
            default:
                LOGGER.info("Provided query is not yet supported by this tool.");
                break;
        }

        long endTime = System.currentTimeMillis();
        LOGGER.debug("Execution completed for query - '{}' on {}", query, new Date(endTime));
        LOGGER.debug("Total execution time for query - '{}' is {}ms.", query, (endTime - startTime));
        LOGGER.info("Query executed in {}ms.", (endTime - startTime));

        EventLogger.log("Execution completed for query - '" + query + "'");

        GeneralLogger.log("Query '" + query + "' executed in " + (endTime - startTime) + "ms");
        GeneralLogger.logDatabaseState();
    }
}
