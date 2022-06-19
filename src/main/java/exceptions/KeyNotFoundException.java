package exceptions;

public class KeyNotFoundException extends Exception {
    public static final long serialVersionUID = 4328742;

    public KeyNotFoundException(final String errorMessage) {
        super(errorMessage);
    }
}
