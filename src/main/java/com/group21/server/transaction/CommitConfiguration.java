package com.group21.server.transaction;

public class CommitConfiguration {

    private static CommitConfiguration commitConfiguration = null;
    private boolean autoCommitValue = true;

    private CommitConfiguration() {
    }

    public static CommitConfiguration getInstance() {
        if (commitConfiguration == null) {
            commitConfiguration = new CommitConfiguration();
        }
        return commitConfiguration;
    }

    public boolean isAutoCommitValue() {
        return autoCommitValue;
    }

    public void setAutoCommitValue(String query) {
        int indexOfEqual = query.indexOf('=');
        autoCommitValue = Boolean.parseBoolean(query.substring(indexOfEqual + 2).replace(";", ""));
    }
}