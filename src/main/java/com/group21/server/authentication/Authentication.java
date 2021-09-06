package com.group21.server.authentication;

import java.util.Map;

import com.group21.utils.FileReader;
import com.group21.utils.PasswordVerifier;

public class Authentication {
    public boolean login(String username, String password) {
        Map<String, String> authenticationMap = FileReader.readAuthenticationFile();

        if (authenticationMap.containsKey(username)) {
            String encryptedStoredPassword = authenticationMap.get(username);
            return PasswordVerifier.verify(password, encryptedStoredPassword);
        } else {
            return false;
        }
    }
}
