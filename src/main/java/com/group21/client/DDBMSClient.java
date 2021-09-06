package com.group21.client;

import java.util.Scanner;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.group21.configurations.ApplicationConfiguration;
import com.group21.server.authentication.Authentication;
import com.group21.server.logger.EventLogger;
import com.group21.server.models.DatabaseSite;
import com.group21.server.processor.QueryProcessor;
import com.group21.server.erd.ERDGenerator;
import com.group21.server.sqldump.SqlDumpGenerator;
import com.group21.server.transaction.CommitConfiguration;
import com.group21.server.transaction.TransactionExecutor;
import com.group21.utils.RemoteDatabaseConnection;
import com.group21.utils.RemoteDatabaseReader;
import com.group21.utils.RemoteDatabaseWriter;

public class DDBMSClient {
    private static final Logger LOGGER = LoggerFactory.getLogger(DDBMSClient.class);

    public static void main(String[] args) {
        LOGGER.info("   .-----------------------------.                  ");
        LOGGER.info("   |  Welcome to Group 21 DDBMS  |                  ");
        LOGGER.info("   '-----------------------------'                  ");
        LOGGER.info("                  |                                 ");
        LOGGER.info("                  |                                 ");
        LOGGER.info("                  |                 (\\_/)          ");
        LOGGER.info("                  '---------------- (O.O)           ");
        LOGGER.info("                                    (> <)           ");
        LOGGER.info("");

        DDBMSSetup.perform();

        Scanner scanner = new Scanner(System.in);

        LOGGER.info("Enter Username : ");
        String username = scanner.nextLine().trim();

        LOGGER.info("Enter Password : ");
        String password = scanner.nextLine().trim();

        Authentication authentication = new Authentication();
        boolean isValidUser = authentication.login(username, password);

        if (!isValidUser) {
            LOGGER.error("Invalid username or password.");
            System.exit(0);
            return;
        }

        LOGGER.info("");

        while (true) {
            try {
                LOGGER.info("DDBMS>>");

                String userInput = scanner.nextLine();
                String command;

                CommitConfiguration commitConfiguration = CommitConfiguration.getInstance();

                if (userInput.matches("^export sqldump;?")) {
                    command = "sqldump";
                } else if (userInput.matches("^export erd;?$")) {
                    command = "erd";
                } else if (userInput.matches("^set auto_commit = (true|false);?")) {
                    command = "set auto_commit";
                } else if (userInput.equals("commit") || userInput.equals("commit;")) {
                    command = "commit";
                } else if (userInput.equals("rollback") || userInput.equals("rollback;")) {
                    command = "rollback";
                } else {
                    command = userInput.trim();
                }

                switch (command) {
                    case "":
                        if (ApplicationConfiguration.CURRENT_SITE == DatabaseSite.LOCAL) {
                            // This is done only for local as remote site can not access local machine
                            RemoteDatabaseReader.syncDistributedDataDictionary();
                        }
                        break;
                    case "help":
                        LOGGER.info("Below are some available options:");
                        LOGGER.info("\texport sqldump               - To get table structure DDLs");
                        LOGGER.info("\texport erd                   - To get Textual ER Diagram");
                        LOGGER.info("\tValid SQL Query              - To execute valid SQL queries");
                        LOGGER.info("\texit                         - To exit DDBMS client");
                        LOGGER.info("\tset auto_commit = true/false - To change auto commit flag (Default - true)");
                        LOGGER.info("\tcommit                       - To commit transaction");
                        LOGGER.info("\trollback                     - To rollback transaction");
                        break;
                    case "sqldump":
                        SqlDumpGenerator.generate();
                        break;
                    case "erd":
                        ERDGenerator.generate();
                        break;
                    case "exit":
                        if (!commitConfiguration.isAutoCommitValue()) {
                            TransactionExecutor.rollbackTransaction();
                        }

                        RemoteDatabaseConnection.closeSession();
                        System.exit(0);
                        return;
                    case "set auto_commit":
                        commitConfiguration.setAutoCommitValue(userInput);
                        break;
                    case "commit":
                        if (!commitConfiguration.isAutoCommitValue()) {
                            TransactionExecutor.executeTransaction();
                        } else {
                            LOGGER.info("Nothing to commit as auto commit is already on!");
                        }
                        break;
                    case "rollback":
                        if (!commitConfiguration.isAutoCommitValue()) {
                            TransactionExecutor.rollbackTransaction();
                        } else {
                            LOGGER.info("Nothing to commit as auto commit is already on!");
                        }
                        break;
                    default:
                        QueryProcessor.process(command, commitConfiguration.isAutoCommitValue());
                        if (ApplicationConfiguration.CURRENT_SITE == DatabaseSite.LOCAL) {
                            // This is done only for local as remote site can not access local machine
                            RemoteDatabaseWriter.syncDistributedDataDictionary();
                        }
                        break;
                }
                LOGGER.info("");
            } catch (Exception e) {
                LOGGER.error("Error occurred while execution : ", e);
                EventLogger.error(e.getMessage());
                return;
            }
        }
    }
}
