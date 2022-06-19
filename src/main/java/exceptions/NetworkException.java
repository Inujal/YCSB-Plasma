package exceptions;

public class NetworkException extends Exception {
    public static final long serialVersionUID = 4328747;

    public NetworkException(final String errorMessage) {
        super(errorMessage);
    }
}
