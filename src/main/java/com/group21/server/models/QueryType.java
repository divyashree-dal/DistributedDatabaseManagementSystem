package com.group21.server.models;

public enum QueryType {
    CREATE,
    INSERT,
    UPDATE,
    SELECT,
    DELETE,
    DROP,
    TRUNCATE,
    UNKNOWN;

    public static QueryType from(String query) {
        int indexOfSpace = query.indexOf(' ');
        if (indexOfSpace == -1) {
            return UNKNOWN;
        }

        String queryType = query.substring(0, indexOfSpace);

        for (QueryType type : values()) {
            if (type.name().equalsIgnoreCase(queryType)) {
                return type;
            }
        }
        return UNKNOWN;
    }
}
