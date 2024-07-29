package edu.utexas.tacc.tapis.files.lib.exceptions;

public class UnrecoverableTransferException extends Exception {
    public UnrecoverableTransferException() {
        super();
    }

    public UnrecoverableTransferException(String message) {
        super(message);
    }

    public UnrecoverableTransferException(String message, Throwable cause) {
        super(message, cause);
    }

    public UnrecoverableTransferException(Throwable cause) {
        super(cause);
    }


    public UnrecoverableTransferException(String message, Throwable cause, boolean enableSuppression, boolean writeableStackTrace) {
        super(message, cause, enableSuppression, writeableStackTrace);
    }

}