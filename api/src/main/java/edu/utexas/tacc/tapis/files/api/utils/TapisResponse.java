package edu.utexas.tacc.tapis.files.api.utils;

import edu.utexas.tacc.tapis.shared.utils.TapisUtils;

public class TapisResponse<T> {

    private int status = 200;
    private String message = "";
    private T result;
    private final String version = TapisUtils.getTapisVersion();

    public int getStatus() {
        return status;
    }

    public void setStatus(int status) {
        this.status = status;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public T getResult() {
        return result;
    }

    public void setResult(T result) {
        this.result = result;
    }

    public String getVersion() {
        return  version;
    }
}
