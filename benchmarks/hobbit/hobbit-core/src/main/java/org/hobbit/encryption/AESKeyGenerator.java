package org.hobbit.encryption;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.crypto.SecretKey;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.PBEKeySpec;
import javax.crypto.spec.SecretKeySpec;
import java.security.NoSuchAlgorithmException;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.KeySpec;

/**
 * This class generates keys from password and salt for Java AES encryption.
 *
 * @author Ivan Ermilov (iermilov@informatik.uni-leipzig.de)
 *
 */
public class AESKeyGenerator {
    private static final Logger LOGGER = LoggerFactory.getLogger(AESKeyGenerator.class);

    public static SecretKey generate(String password, String salt) {
        SecretKeyFactory factory = null;
        try {
            factory = SecretKeyFactory.getInstance("PBKDF2WithHmacSHA1");
        } catch (NoSuchAlgorithmException e) {
            LOGGER.error("Could not get encryption algorithm PBKDF2WithHmacSHA1 for key generation.");
        }
        KeySpec spec = new PBEKeySpec(password.toCharArray(), salt.getBytes(), 65536, 256);
        SecretKey tmp = null;
        try {
            tmp = factory.generateSecret(spec);
        } catch (InvalidKeySpecException e) {
            LOGGER.error("Invalid KeySpec when generating SecretKey.");
        }
        SecretKey secret = new SecretKeySpec(tmp.getEncoded(), "AES");

        return secret;
    }
}
