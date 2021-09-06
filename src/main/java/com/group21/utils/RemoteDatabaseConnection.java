package com.group21.utils;

import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.group21.configurations.ApplicationConfiguration;
import com.group21.server.logger.EventLogger;
import com.jcraft.jsch.Channel;
import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;

public class RemoteDatabaseConnection {

    private static final Logger LOGGER = LoggerFactory.getLogger(RemoteDatabaseConnection.class);

    private static Session session = null;
    private static ChannelSftp sftpChannel = null;

    private static Session createSession() {

        try {
            JSch jsch = new JSch();
            jsch.addIdentity(ApplicationConfiguration.PRIVATE_KEY_FILE_PATH);

            Properties config = new Properties();
            config.put("StrictHostKeyChecking", "no");

            Session session = jsch.getSession(ApplicationConfiguration.REMOTE_DB_USER, ApplicationConfiguration.REMOTE_DB_HOST);
            session.setConfig(config);
            session.connect();

            return session;
        } catch (JSchException e) {
            LOGGER.error("Error occurred while connecting to remote database.");
            EventLogger.error(e.getMessage());
        }
        return null;
    }

    private static ChannelSftp createChannel() {
        if (session == null) {
            session = createSession();
        }

        if (session == null) {
            return null;
        }

        if (sftpChannel == null) {
            try {
                Channel channel = session.openChannel("sftp");
                channel.connect();
                sftpChannel = (ChannelSftp) channel;
            } catch (JSchException exception) {
                LOGGER.error("Error while creating sftp channel.");
                EventLogger.error(exception.getMessage());
            }
        }
        return sftpChannel;
    }

    public static ChannelSftp getSftpChannel() {
        return createChannel();
    }

    public static void closeSession() {
        if (sftpChannel != null) {
            sftpChannel.disconnect();
        }

        if (session != null) {
            session.disconnect();
        }
    }
}
