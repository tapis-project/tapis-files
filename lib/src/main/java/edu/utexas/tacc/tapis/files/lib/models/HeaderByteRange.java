package edu.utexas.tacc.tapis.files.lib.models;

import edu.utexas.tacc.tapis.files.lib.utils.Utils;
import edu.utexas.tacc.tapis.shared.utils.TapisUtils;

import javax.validation.ValidationException;

public class HeaderByteRange
{
    private long min;
    private long max;

    public HeaderByteRange(String hparms)
    {
      try
      {
        String[] params = hparms.split(",");
        min = Long.parseLong(params[0]);
        max = Long.parseLong(params[1]);
        if (min > max) throw new ValidationException(Utils.getMsg("FILES_RANGE1", hparms));
      }
      catch (Exception ex) { throw new ValidationException(Utils.getMsg("FILES_RANGE2", hparms)); }
    }

  public long getMin() { return min; }
  public void setMin(long l) { min = l; }

  public long getMax() { return max; }
  public void setMax(long l) { max = l; }

  @Override
  public String toString() { return TapisUtils.toString(this); }
}