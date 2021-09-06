package com.group21.server.models;

import org.apache.logging.log4j.util.Strings;

import com.group21.utils.RegexUtil;

public enum Constraint {
    PRIMARY_KEY("PRIMARY KEY"),
    FOREIGN_KEY("FOREIGN KEY"),
    UNKNOWN;

    private static final String PRIMARY_KEY_REGEX = "^[a-zA-Z_]* %s PRIMARY KEY$";
    private static final String FOREIGN_KEY_REGEX = "^[a-zA-Z_]* %s FOREIGN KEY REFERENCES .*\\(.*\\)$";

    private final String keyword;

    Constraint() {
        this(null);
    }

    Constraint(String keyword) {
        if (keyword == null) {
            this.keyword = name();
        } else {
            this.keyword = keyword;
        }
    }

    public String getKeyword() {
        return keyword;
    }

    public static Constraint from(String name) {
        for (Constraint constraint : values()) {
            if (constraint.name().equalsIgnoreCase(name)) {
                return constraint;
            }
        }
        return UNKNOWN;
    }

    public static boolean validPrimaryKeySyntax(String primaryKeyData) {
        String allowedTypesRegex = DataType.getAllowedTypesRegex();
        String matchedPrimaryKeyData = RegexUtil.getMatch(primaryKeyData, String.format(PRIMARY_KEY_REGEX, allowedTypesRegex));
        return !Strings.isBlank(matchedPrimaryKeyData);
    }

    public static boolean validForeignKeySyntax(String primaryKeyData) {
        String allowedTypesRegex = DataType.getAllowedTypesRegex();
        String matchedPrimaryKeyData = RegexUtil.getMatch(primaryKeyData, String.format(FOREIGN_KEY_REGEX, allowedTypesRegex));
        return !Strings.isBlank(matchedPrimaryKeyData);
    }
}
