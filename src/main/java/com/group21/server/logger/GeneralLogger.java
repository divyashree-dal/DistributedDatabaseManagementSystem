package com.group21.server.logger;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;

import com.group21.configurations.ApplicationConfiguration;
import com.group21.server.models.DatabaseSite;
import com.group21.server.models.TableInfo;
import com.group21.utils.FileReader;
import com.group21.utils.RemoteDatabaseReader;

public class GeneralLogger {

    private static final DateTimeFormatter logTimeFormatter = DateTimeFormatter.ofPattern("dd-MM-yyyy HH:mm:ss");

    private static final Path generalLogFilePath = Paths.get(ApplicationConfiguration.LOG_DIRECTORY + ApplicationConfiguration.FILE_SEPARATOR + ApplicationConfiguration.GENERAL_LOG_FILE_NAME);

    public static void log(String content) {
        LocalDateTime currentDateTime = LocalDateTime.now();
        String formattedDate = currentDateTime.format(logTimeFormatter);

        String logContent = "INFO | " + formattedDate + " | " + content + ApplicationConfiguration.NEW_LINE;

        try {
            Files.write(generalLogFilePath, logContent.getBytes(), StandardOpenOption.APPEND);
        } catch (Exception e) {
            System.out.println("Error occurred while writing to log file.");
        }
    }

    public static void logDatabaseState() {
        List<TableInfo> localTableInfo = FileReader.readLocalDataDictionary();

        LocalDateTime currentDateTime = LocalDateTime.now();
        String formattedDate = currentDateTime.format(logTimeFormatter);

        StringBuilder logContent = new StringBuilder("INFO | " + formattedDate + " | Below is current database state" + ApplicationConfiguration.NEW_LINE);
        logContent.append("\t").append(localTableInfo.size()).append(" tables are available at ").append(ApplicationConfiguration.CURRENT_SITE.name()).append(" site.").append(ApplicationConfiguration.NEW_LINE);

        if (localTableInfo.size() > 0) {
            logContent.append("\t").append("Table Name | Number of Rows" + ApplicationConfiguration.NEW_LINE);
            for (TableInfo tableInfo : localTableInfo) {
                logContent.append("\t").append(tableInfo.getTableName()).append(" | ").append(tableInfo.getNumberOfRows()).append(ApplicationConfiguration.NEW_LINE);
            }
        }
        logContent.append(ApplicationConfiguration.NEW_LINE);

        if (ApplicationConfiguration.CURRENT_SITE == DatabaseSite.LOCAL) {
            List<TableInfo> remoteTableInfo = RemoteDatabaseReader.readLocalDataDictionary();
            logContent.append("\t").append(remoteTableInfo.size()).append(" tables are available at REMOTE site.").append(ApplicationConfiguration.NEW_LINE);

            if (remoteTableInfo.size() > 0) {
                logContent.append("\t").append("Table Name | Number of Rows" + ApplicationConfiguration.NEW_LINE);
                for (TableInfo tableInfo : remoteTableInfo) {
                    logContent.append("\t").append(tableInfo.getTableName()).append(" | ").append(tableInfo.getNumberOfRows()).append(ApplicationConfiguration.NEW_LINE);
                }
            }
        }
        logContent.append(ApplicationConfiguration.NEW_LINE);

        try {
            Files.write(generalLogFilePath, logContent.toString().getBytes(), StandardOpenOption.APPEND);
        } catch (Exception e) {
            System.out.println("Error occurred while writing to log file.");
        }
    }
}
