package edu.utexas.tacc.tapis.files.lib.models;

import java.time.Instant;


/*
 * Class containing information returned by the Linux stat command
 */
public class FileStatInfo
{
  private final String absolutePath;
  private final int uid;
  private final int gid;
  private final long size;
  private final String perms;
  private final Instant accessTime;
  private final Instant modifyTime;
  private final boolean isDir;
  private final boolean isLink;

  public FileStatInfo(String path1, int uid1, int gid1, long size1, int perms1,
                      int aTime, int mTime, boolean isDir1, boolean isLink1)
  {
    absolutePath = path1;
    uid = uid1;
    gid = gid1;
    size = size1;
    perms = Integer.toOctalString(perms1);
    // TODO convert times from int to Instant
    accessTime = null;
    modifyTime = null;
    isDir = isDir1;
    isLink = isLink1;
  }

  public String getAbsolutePath() {
        return absolutePath;
    }
  public int getUid() { return uid; }
  public int getGid() { return gid; }
  public long getSize() { return size; }
  public Instant getAccessTime() { return accessTime; }
  public Instant getModifyTime() { return modifyTime; }
  public String getPerms() {
        return perms;
    }
  public boolean isDir() { return isDir; }
  public boolean isLink() { return isLink; }
}