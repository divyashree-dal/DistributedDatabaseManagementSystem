package com.group21.server.models;

import java.util.ArrayList;
import java.util.List;

public enum DataType {
    INT,
    DOUBLE,
    TEXT,
    UNKNOWN;

    public static DataType from(String type) {
        for (DataType dataType : values()) {
            if (dataType.name().equalsIgnoreCase(type)) {
                return dataType;
            }
        }
        return UNKNOWN;
    }

    public static List<String> allowedTypes() {
        List<String> allowedTypes = new ArrayList<>();
        allowedTypes.add(INT.name());
        allowedTypes.add(DOUBLE.name());
        allowedTypes.add(TEXT.name());

        return allowedTypes;
    }

    public static String getAllowedTypesRegex() {
        return "(INT|DOUBLE|TEXT)";
    }
}
