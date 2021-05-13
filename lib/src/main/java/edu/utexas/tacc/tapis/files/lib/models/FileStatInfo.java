package edu.utexas.tacc.tapis.files.lib.models;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.swagger.v3.oas.annotations.media.Schema;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;

/*
 * Class containing information returned by the Linux stat command
 */
public class FileStatInfo
{
  private String absolutePath;
  private int uid;
  private int gid;
  private long size;
  private String perms;
  private Instant accessTime;
  private Instant modifyTime;
//  private LocalDateTime accessTime;
//  private LocalDateTime modifyTime;
  @JsonProperty("dir")
  private boolean isDir;
  @JsonProperty("link")
  private boolean isLink;

  public FileStatInfo() { }

  public FileStatInfo(String path1, int uid1, int gid1, long size1, String perms1,
                      int aTime, int mTime, boolean isDir1, boolean isLink1)
  {
    absolutePath = path1;
    uid = uid1;
    gid = gid1;
    size = size1;
    perms = perms1;
    accessTime = LocalDateTime.ofEpochSecond(aTime, 0, ZoneOffset.UTC).toInstant(ZoneOffset.UTC);
//    accessTime = LocalDateTime.ofEpochSecond(aTime, 0, ZoneOffset.UTC);
    modifyTime = LocalDateTime.ofEpochSecond(mTime, 0, ZoneOffset.UTC).toInstant(ZoneOffset.UTC);
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
//  public LocalDateTime getAccessTime() { return accessTime; }
//  public void setAccessTime(Instant i) { accessTime = i; }
//  public void setAccessTime(String s) { accessTime = LocalDateTime.ofInstant(Instant.parse(s), ZoneOffset.UTC); }
  public void setAccessTime(String s) { accessTime = Instant.parse(s); }
  public Instant getModifyTime() { return modifyTime; }
  public void setModifyTime(Instant i) { modifyTime = i; }
  public void setModifyTime(String s) {
    modifyTime = Instant.parse(s);
  }
  public String getPerms() {
        return perms;
    }
  public boolean isDir() { return isDir; }
  public boolean isLink() { return isLink; }
}