package org.hobbit.encryption;

import org.junit.Test;

import static org.junit.Assert.*;

public class AESTest {

    @Test
    public void encryptDecrypt() throws AESException {
        String password = "password";
        String salt = "salt";
        AES encryption_1 = new AES(password, salt);
        String toEncrypt = "hello world!";
        byte[] encrypted = encryption_1.encrypt(toEncrypt);

        AES encryption_2 = new AES(password, salt);
        String decrypted = new String(encryption_2.decrypt(encrypted));
        assertTrue(decrypted.equals(toEncrypt));
    }
}
