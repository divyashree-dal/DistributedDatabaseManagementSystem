package com.group21.utils;

import java.security.NoSuchAlgorithmException;
import java.security.spec.InvalidKeySpecException;
import java.util.Base64;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.PBEKeySpec;

public class PasswordVerifier {
    private static final Logger LOGGER = LoggerFactory.getLogger(PasswordVerifier.class);

    private static final String HASH_ALGORITHM = "PBKDF2WithHmacSHA1";
    private static final String SALT = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9";
    private static final int ITERATIONS = 512;
    private static final int KEY_LENGTH = 128;

    private PasswordVerifier() {
    }

    public static byte[] hash(char[] password) {
        PBEKeySpec pbeKeySpec = new PBEKeySpec(password, SALT.getBytes(), ITERATIONS, KEY_LENGTH);
        try {
            SecretKeyFactory secretKeyFactory = SecretKeyFactory.getInstance(HASH_ALGORITHM);
            return secretKeyFactory.generateSecret(pbeKeySpec).getEncoded();
        } catch (NoSuchAlgorithmException | InvalidKeySpecException e) {
            LOGGER.error("Error occurred while generating password hash.");
            throw new SecurityException();
        }
    }

    public static boolean verify(String providedPassword, String storedPassword) {
        byte[] passwordHash = hash(providedPassword.toCharArray());
        String encodedPassword = Base64.getEncoder().encodeToString(passwordHash);
        return encodedPassword.equalsIgnoreCase(storedPassword);
    }
}