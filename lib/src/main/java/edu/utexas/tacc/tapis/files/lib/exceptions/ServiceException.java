package edu.utexas.tacc.tapis.files.lib.exceptions;

public class ServiceException extends Exception {
    public ServiceException(String message) {super(message);}
    public ServiceException(String message, Throwable err) {super(message, err);}
}
