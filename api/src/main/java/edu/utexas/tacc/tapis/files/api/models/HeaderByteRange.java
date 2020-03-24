package edu.utexas.tacc.tapis.files.api.models;

import javax.validation.ValidationException;

public class HeaderByteRange {
    private long min;
    private long max;

    public HeaderByteRange(String hparms) {
        try {
            String[] params = hparms.split(",");
            min = Long.parseLong(params[0]);
            max = Long.parseLong(params[1]);
            if (min > max) {
                throw new ValidationException("Invalid range, min > max");
            }
        } catch (Exception ex) {
            throw new ValidationException("Invalid range, must be in the format of range=min,max");
        }

    }

    public long getMin() {
        return min;
    }

    public void setMin(long min) {
        this.min = min;
    }

    public long getMax() {
        return max;
    }

    public void setMax(long max) {
        this.max = max;
    }

}