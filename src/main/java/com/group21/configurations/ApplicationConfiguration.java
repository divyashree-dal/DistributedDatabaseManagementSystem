package com.group21.configurations;

import com.group21.server.models.DatabaseSite;

public class ApplicationConfiguration {
    private ApplicationConfiguration() {
    }

    public static final DatabaseSite CURRENT_SITE = DatabaseSite.LOCAL;
    public static final String DATA_FILE_FORMAT = ".dat";
    public static final String METADATA_FILE_FORMAT = ".metadata";
    public static final String DELIMITER = "|";
    public static final String DELIMITER_REGEX = "\\|";
    public static final String DATA_DIRECTORY = "DDBMS_21_Data";
    public static final String LOG_DIRECTORY = "logs";
    public static final String AUTHENTICATION_FILE_NAME = "authentication.dat";
    public static final String LOCAL_DATA_DICTIONARY_NAME = "local_data_dictionary.dat";
    public static final String DISTRIBUTED_DATA_DICTIONARY_NAME = "distributed_data_dictionary.dat";
    public static final String ERD_FILE_NAME = "ddbms_group21_erd.txt";
    public static final String FILE_SEPARATOR = System.getProperty("file.separator");
    public static final String NEW_LINE = "\n";
    public static final String SQL_DUMP_FILE_NAME = "sql_dump.sql";
    public static final String TRANSACTION_FILE_NAME = "transaction.txt";
    public static final String GENERAL_LOG_FILE_NAME = "general.log";
    public static final String EVENT_LOG_FILE_NAME = "event.log";

    // Remote Database configuration
    public static final String REMOTE_DB_DATA_DIRECTORY = "/home/kartik_gevariya0003/group21/csci-5408-group-21/DDBMS_21_Data";
    public static final String REMOTE_DB_USER = "kartik_gevariya0003";
    public static final String REMOTE_DB_HOST = "35.223.217.30";
    public static final String PRIVATE_KEY_FILE_PATH = "/Users/kartikgevariya/.ssh/id_rsa_gcp";

}
