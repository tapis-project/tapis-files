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
  private boolean dir;
  private boolean link;

  public FileStatInfo() { }

  public FileStatInfo(String path1, int uid1, int gid1, long size1, int perms1,
                      Instant aTime, Instant mTime, boolean dir1, boolean link1)
  {
    absolutePath = path1;
    uid = uid1;
    gid = gid1;
    size = size1;
    perms = getPermsFromInt(perms1);
    accessTime = aTime;
    modifyTime = mTime;
    dir = dir1;
    link = link1;
  }

  public String getAbsolutePath() {
        return absolutePath;
    }
  public int getUid() { return uid; }
  public int getGid() { return gid; }
  public long getSize() { return size; }
  public Instant getAccessTime() { return accessTime; }
  public void setAccessTime(String s) { accessTime = Instant.parse(s); }
  public Instant getModifyTime() { return modifyTime; }
  public void setModifyTime(Instant i) { modifyTime = i; }
  public void setModifyTime(String s) {
    modifyTime = Instant.parse(s);
  }
  public String getPerms() {
        return perms;
    }
  public boolean isDir() { return dir; }
  public boolean isLink() { return link; }

 public String toString()
 {
   return
      String.format("AbsolutePath: %s%n Uid: %d%n Gid: %d%n Size: %d%n Perms: %s%n AccessTime: %s%n ModifyTime: %s%n isDir: %b%n isLink: %b%n",
           absolutePath, uid, gid, size, perms, accessTime, modifyTime, dir, link);
 }

 /*
  * Convert permissions from int representation to string,
  *   e.g. 755 -> rwxr-xr-x
  */
 private static String getPermsFromInt(int permsAsInt)
 {
   StringBuilder sb = new StringBuilder();
   if ((permsAsInt & 256) != 0) sb.append('r'); else sb.append('-');
   if ((permsAsInt & 128) != 0) sb.append('w'); else sb.append('-');
   if ((permsAsInt & 64) != 0) sb.append('x'); else sb.append('-');
   if ((permsAsInt & 32) != 0) sb.append('r'); else sb.append('-');
   if ((permsAsInt & 16) != 0) sb.append('w'); else sb.append('-');
   if ((permsAsInt & 8) != 0) sb.append('x'); else sb.append('-');
   if ((permsAsInt & 4) != 0) sb.append('r'); else sb.append('-');
   if ((permsAsInt & 2) != 0) sb.append('w'); else sb.append('-');
   if ((permsAsInt & 1) != 0) sb.append('x'); else sb.append('-');
   return sb.toString();
 }
}