package edu.utexas.tacc.tapis.files.lib.models;

import edu.utexas.tacc.tapis.shared.utils.TapisUtils;

import java.util.Objects;

/**
 * Additional user specific share information for a file path
 *
 * This class is intended to represent an immutable object.
 * Please keep it immutable.
 */
public final class UserShareInfo
{
  private final String username; // user with whom the path is shared
  private final String path; // Path that resulted in the share

  /* ********************************************************************** */
  /*                           Constructors                                 */
  /* ********************************************************************** */

  // Default constructor to set defaults. This appears to be needed for when object is created from json using gson.
  public UserShareInfo()
  {
    username = null;
    path = null;
  }

  public UserShareInfo(String username1, String path1)
  {
    username = username1;
    path = path1;
  }

  /* ********************************************************************** */
  /*                               Accessors                                */
  /* ********************************************************************** */
  public String getUsername() { return username; }
  public String getPath() { return path; }

  @Override
  public String toString() {return TapisUtils.toString(this);}

  @Override
  public boolean equals(Object o)
  {
    if (o == this) return true;
    // Note: no need to check for o==null since instanceof will handle that case
    if (!(o instanceof UserShareInfo)) return false;
    var that = (UserShareInfo) o;
    return (Objects.equals(this.username, that.username) && Objects.equals(this.path, that.path));
  }

  @Override
  public int hashCode()
  {
    int retVal = (username == null ? 1 : username.hashCode());
    retVal = 31 * retVal + (path == null ? 0 : path.hashCode());
    return retVal;
  }
}