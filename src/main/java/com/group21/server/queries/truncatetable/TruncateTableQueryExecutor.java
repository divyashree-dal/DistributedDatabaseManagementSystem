package com.group21.server.queries.truncatetable;

import com.group21.server.queries.delete.DeleteQueryExecutor;

public class TruncateTableQueryExecutor {

    private final TruncateTableParser truncateTableParser;

    public TruncateTableQueryExecutor() {
        this.truncateTableParser = new TruncateTableParser();
    }

    public void execute(String query) {
        boolean isQueryValid = truncateTableParser.isValid(query);

        if (isQueryValid) {
            String tableName = truncateTableParser.getTableName(query);

            DeleteQueryExecutor deleteQueryExecutor = new DeleteQueryExecutor();
            deleteQueryExecutor.execute("DELETE FROM " + tableName, true);
        }
    }
}
