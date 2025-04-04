package org.hobbit.encryption;

public class AESException extends Exception {

    private static final long serialVersionUID = 1L;

    private static String defaultMessage = "Error initializing encryption algorithm " + AES.CIPHER_ALGORITHM + ". "
            + "Please check your Java distribution for the availability of this type of encryption. "
            + "To fall back to unencrypted communication between storage-service and other services "
            + "unset AES_PASSWORD and AES_SALT environmental variables.";

    public AESException() {
        super(defaultMessage);
    }
}
