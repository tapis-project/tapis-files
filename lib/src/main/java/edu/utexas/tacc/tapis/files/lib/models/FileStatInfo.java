package edu.utexas.tacc.tapis.files.lib.models;

import java.time.LocalDateTime;
import java.time.ZoneOffset;

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
  private final LocalDateTime accessTime;
  private final LocalDateTime modifyTime;
  private final boolean isDir;
  private final boolean isLink;

  public FileStatInfo(String path1, int uid1, int gid1, long size1, String perms1,
                      int aTime, int mTime, boolean isDir1, boolean isLink1)
  {
    absolutePath = path1;
    uid = uid1;
    gid = gid1;
    size = size1;
    perms = perms1;
    accessTime = LocalDateTime.ofEpochSecond(aTime, 0, ZoneOffset.UTC);
    modifyTime = LocalDateTime.ofEpochSecond(mTime, 0, ZoneOffset.UTC);
    isDir = isDir1;
    isLink = isLink1;
  }

  public String getAbsolutePath() {
        return absolutePath;
    }
  public int getUid() { return uid; }
  public int getGid() { return gid; }
  public long getSize() { return size; }
  public LocalDateTime getAccessTime() { return accessTime; }
  public LocalDateTime getModifyTime() { return modifyTime; }
  public String getPerms() {
        return perms;
    }
  public boolean isDir() { return isDir; }
  public boolean isLink() { return isLink; }
}