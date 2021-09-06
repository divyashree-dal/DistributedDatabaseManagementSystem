package com.group21.server.models;

import java.util.List;

import com.group21.utils.FileReader;
import com.group21.utils.FileWriter;
import com.group21.utils.RemoteDatabaseReader;
import com.group21.utils.RemoteDatabaseWriter;

public enum DatabaseSite {
    LOCAL {
        @Override
        public void writeFile(String fileName, String fileContent) {
            FileWriter.writeFile(fileName, fileContent);
        }

        @Override
        public List<TableInfo> readLocalDataDictionary() {
            return FileReader.readLocalDataDictionary();
        }

        @Override
        public void writeLocalDataDictionary(TableInfo tableInfo) {
            FileWriter.writeLocalDataDictionary(tableInfo);
        }

        @Override
        public void writeMetadata(String tableName, List<Column> columnDetails) {
            FileWriter.writeMetadata(tableName, columnDetails);
        }

        @Override
        public List<Column> readMetadata(String tableName) {
            return FileReader.readMetadata(tableName);
        }

        @Override
        public void writeData(String tableName, List<String> columnData) {
            FileWriter.writeData(tableName, columnData);
        }

        @Override
        public void deleteTable(String tableName) {
            FileWriter.deleteTable(tableName);
        }

        @Override
        public List<String> readColumnMetadata(String tableName) {
            return FileReader.readColumnMetadata(tableName);
        }

        @Override
        public List<String> readData(String tableName) {
            return FileReader.readData(tableName);
        }

        @Override
        public void deleteOnlyTable(String tableName) {
            FileWriter.deleteOnlyTable(tableName);
        }

        @Override
        public List<String> readColumnData(String tableName, String columnName) {
            return FileReader.readColumnData(tableName, columnName);
        }

        @Override
        public void incrementRowCountInLocalDataDictionary(String tableName) {
            FileWriter.incrementRowCountInLocalDataDictionary(tableName);
        }

        @Override
        public void decrementRowCountInLocalDataDictionary(String tableName, int count) {
            FileWriter.decrementRowCountInLocalDataDictionary(tableName, count);
        }
    },
    REMOTE {
        @Override
        public void writeFile(String fileName, String fileContent) {
            RemoteDatabaseWriter.writeFile(fileName, fileContent);
        }

        @Override
        public List<TableInfo> readLocalDataDictionary() {
            return RemoteDatabaseReader.readLocalDataDictionary();
        }

        @Override
        public void writeLocalDataDictionary(TableInfo tableInfo) {
            RemoteDatabaseWriter.writeLocalDataDictionary(tableInfo);
        }

        @Override
        public void writeMetadata(String tableName, List<Column> columnDetails) {
            RemoteDatabaseWriter.writeMetadata(tableName, columnDetails);
        }

        @Override
        public List<Column> readMetadata(String tableName) {
            return RemoteDatabaseReader.readMetadata(tableName);
        }

        @Override
        public void writeData(String tableName, List<String> columnData) {
            RemoteDatabaseWriter.writeData(tableName, columnData);
        }

        @Override
        public void deleteTable(String tableName) {
            RemoteDatabaseWriter.deleteTable(tableName);
        }

        @Override
        public List<String> readColumnMetadata(String tableName) {
            return RemoteDatabaseReader.readColumnMetadata(tableName);
        }

        @Override
        public List<String> readData(String tableName) {
            return RemoteDatabaseReader.readData(tableName);
        }

        @Override
        public void deleteOnlyTable(String tableName) {
            RemoteDatabaseWriter.deleteOnlyTable(tableName);
        }

        @Override
        public List<String> readColumnData(String tableName, String columnName) {
            return RemoteDatabaseReader.readColumnData(tableName, columnName);
        }

        @Override
        public void incrementRowCountInLocalDataDictionary(String tableName) {
            RemoteDatabaseWriter.incrementRowCountInLocalDataDictionary(tableName);
        }

        @Override
        public void decrementRowCountInLocalDataDictionary(String tableName, int count) {
            RemoteDatabaseWriter.decrementRowCountInLocalDataDictionary(tableName, count);
        }
    };

    public abstract void writeFile(String fileName, String fileContent);

    public abstract List<TableInfo> readLocalDataDictionary();

    public abstract void writeLocalDataDictionary(TableInfo tableInfo);

    public abstract void writeMetadata(String tableName, List<Column> columnDetails);

    public abstract List<Column> readMetadata(String tableName);

    public abstract void writeData(String tableName, List<String> columnData);

    public abstract void deleteTable(String tableName);

    public abstract List<String> readColumnMetadata(String tableName);

    public abstract List<String> readData(String tableName);

    public abstract void deleteOnlyTable(String tableName);

    public abstract List<String> readColumnData(String tableName, String columnName);

    public abstract void incrementRowCountInLocalDataDictionary(String tableName);

    public abstract void decrementRowCountInLocalDataDictionary(String tableName, int count);

    public static DatabaseSite from(String siteName) {
        for (DatabaseSite databaseSite : values()) {
            if (databaseSite.name().equalsIgnoreCase(siteName)) {
                return databaseSite;
            }
        }
        return LOCAL;
    }
}
