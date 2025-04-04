package org.hobbit.encryption;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.crypto.*;
import javax.crypto.spec.SecretKeySpec;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;

/**
 * This class is a wrapper for Java AES encryption.
 *
 * @author Ivan Ermilov (iermilov@informatik.uni-leipzig.de)
 *
 */

public class AES {
    private static final Logger LOGGER = LoggerFactory.getLogger(AES.class);

    private SecretKey secretKey = null;
    protected static final String CIPHER_ALGORITHM = "AES/ECB/PKCS5PADDING";

    public AES(String password, String salt) {
        this.secretKey = AESKeyGenerator.generate(password, salt);
    }

    public byte[] encrypt(String input) throws AESException {
        byte[] toEncrypt = input.getBytes();
        byte[] encrypted = null;

        Cipher cipher = null;
        try {
            cipher = this.getCipherInstance();
            cipher.init(Cipher.ENCRYPT_MODE, this.secretKey);
        } catch (InvalidKeyException e) {
            LOGGER.error("Could not init ENCRYPT_MODE. Invalid key.", e);
            throw new AESException();
        }

        try {
            encrypted = cipher.doFinal(toEncrypt);
        } catch (IllegalBlockSizeException e) {
            LOGGER.error("Could not encrypt message. Illegal block size.", e);
            throw new AESException();
        } catch (BadPaddingException e) {
            LOGGER.error("Could not encrypt message. Bad padding.", e);
            throw new AESException();
        }

        return encrypted;
    }

    public byte[] decrypt(byte[] input) throws AESException {
        SecretKeySpec spec = new SecretKeySpec(secretKey.getEncoded(), "AES");

        Cipher cipher = null;
        try {
            cipher = this.getCipherInstance();
            cipher.init(Cipher.DECRYPT_MODE, spec);
        } catch (InvalidKeyException e) {
            LOGGER.error("Could not init DECRYPT_MODE. Invalid key.", e);
            throw new AESException();
        }

        byte[] decrypted = new byte[0];
        try {
            decrypted = cipher.doFinal(input);
        } catch (IllegalBlockSizeException e) {
            LOGGER.error("Could not init DECRYPT_MODE. Illegal block size.", e);
            throw new AESException();
        } catch (BadPaddingException e) {
            LOGGER.error("Could not init DECRYPT_MODE. Bad padding.", e);
            throw new AESException();
        }
        return decrypted;
    }

    private Cipher getCipherInstance() throws AESException {
        Cipher cipher = null;
        try {
            cipher = Cipher.getInstance(CIPHER_ALGORITHM);
        } catch (NoSuchAlgorithmException e) {
            LOGGER.error("Error initializing AES encryption. AES algorithm not found.", e);
            throw new AESException();
        } catch (NoSuchPaddingException e) {
            LOGGER.error("Error initializing AES encryption. PKCS5PADDING not found.", e);
            throw new AESException();
        }
        return cipher;
    }
}
