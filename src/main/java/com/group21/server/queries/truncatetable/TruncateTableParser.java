package com.group21.server.queries.truncatetable;

import org.apache.logging.log4j.util.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.group21.utils.RegexUtil;

public class TruncateTableParser {

    private static final Logger LOGGER = LoggerFactory.getLogger(TruncateTableParser.class);

    private static final String TRUNCATE_TABLE_REGEX = "^TRUNCATE TABLE [a-zA-Z_]*;?$";

    public boolean isValid(String query) {
        String matchedQuery = RegexUtil.getMatch(query, TRUNCATE_TABLE_REGEX);

        if (Strings.isBlank(matchedQuery)) {
            LOGGER.error("Syntax error in provided truncate table query.");
            return false;
        }
        return true;
    }

    public String getTableName(String query) {
        int indexOfFirstSpace = query.indexOf(' ');
        int indexOfSecondSpace = query.indexOf(' ', indexOfFirstSpace + 1);

        return query.substring(indexOfSecondSpace + 1).replace(";", "").trim();
    }
}
