package com.group21.constants;

public class CommonRegex {
    private CommonRegex() {
    }

    public static final String INTEGER_REGEX = "[0-9]+";
    public static final String DOUBLE_REGEX = "[0-9]+(\\.[0-9]+)";
    public static final String TEXT_REGEX = "\\'[a-zA-Z_]+\\'";
}
