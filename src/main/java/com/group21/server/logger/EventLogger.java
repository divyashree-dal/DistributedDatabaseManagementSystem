package com.group21.server.logger;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

import com.group21.configurations.ApplicationConfiguration;

public class EventLogger {

    private static final DateTimeFormatter logTimeFormatter = DateTimeFormatter.ofPattern("dd-MM-yyyy HH:mm:ss");

    private static final Path eventLogFilePath = Paths.get(ApplicationConfiguration.LOG_DIRECTORY + ApplicationConfiguration.FILE_SEPARATOR + ApplicationConfiguration.EVENT_LOG_FILE_NAME);

    public static void log(String content) {
        LocalDateTime currentDateTime = LocalDateTime.now();
        String formattedDate = currentDateTime.format(logTimeFormatter);

        String logContent = "INFO | " + formattedDate + " | " + content + ApplicationConfiguration.NEW_LINE;

        try {
            Files.write(eventLogFilePath, logContent.getBytes(), StandardOpenOption.APPEND);
        } catch (Exception e) {
            System.out.println("Error occurred while writing to log file.");
        }
    }

    public static void error(String error) {
        LocalDateTime currentDateTime = LocalDateTime.now();
        String formattedDate = currentDateTime.format(logTimeFormatter);

        String logContent = "ERROR | " + formattedDate + " | " + error + ApplicationConfiguration.NEW_LINE;

        try {
            Files.write(eventLogFilePath, logContent.getBytes(), StandardOpenOption.APPEND);
        } catch (Exception e) {
            System.out.println("Error occurred while writing to log file.");
        }
    }
}
