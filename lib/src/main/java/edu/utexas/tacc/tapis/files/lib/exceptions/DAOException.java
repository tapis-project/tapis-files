package edu.utexas.tacc.tapis.files.lib.exceptions;

public class DAOException extends Exception {
  public DAOException(String message) {
    super(message);
  }

  public DAOException(String message, Throwable cause) {
    super(message, cause);
  }

  public DAOException(Throwable cause) {
    super(cause);
  }

  public DAOException(String message, Throwable cause, boolean enableSuppresion, boolean writableStackTrace) {
    super(message, cause, enableSuppresion, writableStackTrace);
  }
}
