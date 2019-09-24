package edu.utexas.tacc.tapis.files.lib.exceptions;

public class DAOException extends Exception {
  private int errorCode;
  public DAOException(int errorCode) {
    this.errorCode = errorCode;
  }

  public int getErrorCode() {
    return errorCode;
  }
}
