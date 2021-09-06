package com.group21.server.transaction;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.group21.configurations.ApplicationConfiguration;
import com.group21.server.logger.EventLogger;
import com.group21.server.processor.QueryProcessor;
import com.group21.utils.FileReader;

public class TransactionExecutor {

    private static final Logger LOGGER = LoggerFactory.getLogger(TransactionExecutor.class);

    private static final Path transactionFile = Paths.get(ApplicationConfiguration.DATA_DIRECTORY + ApplicationConfiguration.FILE_SEPARATOR + ApplicationConfiguration.TRANSACTION_FILE_NAME);

    public static void executeTransaction() {
        List<String> queries = FileReader.readTransactionFile();
        for (String query : queries) {
            LOGGER.info("Committing data for query '{}'", query);
            QueryProcessor.process(query, true);
            LOGGER.info("");
        }
        try {
            Files.write(transactionFile, "".getBytes(StandardCharsets.UTF_8), StandardOpenOption.TRUNCATE_EXISTING);
        } catch (IOException e) {
            LOGGER.info("Error occurred while truncating transaction file.");
            EventLogger.error(e.getMessage());
        }
    }

    public static void rollbackTransaction() {
        try {
            Files.write(transactionFile, "".getBytes(StandardCharsets.UTF_8), StandardOpenOption.TRUNCATE_EXISTING);
        } catch (IOException e) {
            LOGGER.info("Error occurred while truncating transaction file.");
            EventLogger.error(e.getMessage());
        }
    }
}