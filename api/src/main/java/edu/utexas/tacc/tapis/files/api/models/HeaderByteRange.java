package edu.utexas.tacc.tapis.files.api.models;

import edu.utexas.tacc.tapis.files.lib.utils.Utils;

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
                throw new ValidationException(Utils.getMsg("FILES_RANGE1", hparms));
            }
        } catch (Exception ex) {
            throw new ValidationException(Utils.getMsg("FILES_RANGE2", hparms));
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