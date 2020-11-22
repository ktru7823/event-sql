package usyd.it.olympics;

/*
 * Exception class to enclose other and prevent exposing details to the client
 */


/**
 *
 * @author Bryn
 */
public class OlympicsDBException extends Exception {

    public OlympicsDBException(String message) {
        super(message);
    }

    public OlympicsDBException(String message, Throwable throwable) {
        super(message, throwable);
    }

}
