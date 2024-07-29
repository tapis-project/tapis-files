package edu.utexas.tacc.tapis.files.lib.exceptions;

public class RecoverableTransferException extends Exception{
    public RecoverableTransferException() {
        super();
    }

    public RecoverableTransferException(String message) {
        super(message);
    }

    public RecoverableTransferException(String message, Throwable cause) {
        super(message, cause);
    }

    public RecoverableTransferException(Throwable cause) {
        super(cause);
    }


    public RecoverableTransferException(String message, Throwable cause, boolean enableSuppression, boolean writeableStackTrace) {
        super(message, cause, enableSuppression, writeableStackTrace);
    }

}