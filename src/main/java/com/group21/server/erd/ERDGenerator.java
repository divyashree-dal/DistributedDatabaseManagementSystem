package com.group21.server.erd;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.group21.configurations.ApplicationConfiguration;
import com.group21.server.logger.EventLogger;
import com.group21.server.models.Column;
import com.group21.server.models.Constraint;
import com.group21.server.models.DatabaseSite;
import com.group21.utils.FileReader;

public class ERDGenerator {

    private static final Logger LOGGER = LoggerFactory.getLogger(ERDGenerator.class);

    private static final String ERD_STRING = "%s (%s) --(*)-------- REFERENCES --------(1)--> %s (%s)";

    private ERDGenerator() {
    }

    public static void generate() {
        Map<String, DatabaseSite> gddContentMap = FileReader.readDistributedDataDictionary();

        Path erdFilePath = Paths.get(ApplicationConfiguration.DATA_DIRECTORY + ApplicationConfiguration.FILE_SEPARATOR + ApplicationConfiguration.ERD_FILE_NAME);

        Map<String, Integer> tableAssociationMap = new HashMap<>();
        try {
            if (Files.notExists(erdFilePath)) {
                Files.createFile(erdFilePath);
            } else {
                Files.write(erdFilePath, "".getBytes(), StandardOpenOption.TRUNCATE_EXISTING);
            }

            for (Map.Entry<String, DatabaseSite> gddContentEntry : gddContentMap.entrySet()) {
                String tableName = gddContentEntry.getKey();
                DatabaseSite databaseSite = gddContentEntry.getValue();

                tableAssociationMap.putIfAbsent(tableName, 0);

                List<Column> tableColumns = databaseSite.readMetadata(tableName);

                for (Column column : tableColumns) {
                    Constraint constraint = column.getConstraint();
                    if (constraint == Constraint.FOREIGN_KEY) {
                        String foreignKeyTableName = column.getForeignKeyTable();
                        String foreignKeyColumnName = column.getForeignKeyColumnName();

                        String erdString = String.format(ERD_STRING, tableName, column.getColumnName(), foreignKeyTableName, foreignKeyColumnName) + ApplicationConfiguration.NEW_LINE + ApplicationConfiguration.NEW_LINE;

                        tableAssociationMap.put(tableName, 1);
                        tableAssociationMap.put(foreignKeyTableName, 1);

                        Files.write(erdFilePath, erdString.getBytes(), StandardOpenOption.APPEND);
                    }
                }
            }

            for (Map.Entry<String, Integer> tableInfo : tableAssociationMap.entrySet()) {
                int isTableAssociated = tableInfo.getValue();

                if (isTableAssociated == 0) {
                    String content = tableInfo.getKey() + " is not associated with any table";
                    Files.write(erdFilePath, content.getBytes(), StandardOpenOption.APPEND);
                }
            }

            LOGGER.info("ERD file '{}' generated successfully.", ApplicationConfiguration.ERD_FILE_NAME);
        } catch (IOException exception) {
            LOGGER.info("Error occurred while generating ER Diagram.");
            EventLogger.error(exception.getMessage());
        }
    }
}
