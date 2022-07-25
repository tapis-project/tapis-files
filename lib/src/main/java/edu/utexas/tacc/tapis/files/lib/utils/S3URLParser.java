package edu.utexas.tacc.tapis.files.lib.utils;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * There are 4, yes 4 different flavors of S3 URIs
 */
public class S3URLParser
{
  private static final Pattern p1 = Pattern.compile("(https://)?([^.]+).s3.amazonaws.com/?+");
  private static final Pattern p2 = Pattern.compile("(https://)?([^.]+).s3.([^.]+).amazonaws.com");
  private static final Pattern p3 = Pattern.compile("(https://)?s3.amazonaws.com/([^/]+)");
  private static final Pattern p4 = Pattern.compile("(https://)?s3.([^.]+).amazonaws.com/([^/]+)");

  public static boolean isAWSUrl(String host)
  {
    return p1.matcher(host).matches() ||
           p2.matcher(host).matches() ||
           p3.matcher(host).matches() ||
           p4.matcher(host).matches();
  }

  /*
   *  Any of the following are acceptable S3 URLs
   *  http://bucket.s3.amazonaws.com
   *  http://bucket.s3-aws-region.amazonaws.com
   *  http://s3.amazonaws.com/bucket
   *  http://s3-aws-region.amazonaws.com/bucket
   */
  public static String getRegion(String url)
  {
    Matcher matcher;
    matcher = p1.matcher(url);
    if (matcher.matches()) return null;

    matcher = p2.matcher(url);
    if (matcher.matches()) return matcher.group(3);

    matcher = p3.matcher(url);
    if (matcher.matches()) return null;

    matcher = p4.matcher(url);
    if (matcher.matches()) return matcher.group(2);

    return null;
  }
}
