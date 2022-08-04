package edu.utexas.tacc.tapis.files.lib.models;

import edu.utexas.tacc.tapis.shared.utils.TapisUtils;

import java.util.Objects;

/**
 * TODO Additional user specific share information for a file path
 *
 * This class is intended to represent an immutable object.
 * Please keep it immutable.
 */
public final class UserShareInfo2
{
  private final String username; // user with whom the path is shared
  private final String path; // Path relative to rootDir that resulted in the share
  private final String grantor; // User who granted the share
  private final boolean isPublic; // true if access granted due to public sharing

  /* ********************************************************************** */
  /*                           Constructors                                 */
  /* ********************************************************************** */

  // Default constructor to set defaults. This appears to be needed for when object is created from json using gson.
  public UserShareInfo2()
  {
    username = null;
    path = null;
    grantor = null;
    isPublic = false;
  }

  public UserShareInfo2(String username1, String path1, String grantor1, boolean isPublic1)
  {
    username = username1;
    path = path1;
    grantor = grantor1;
    isPublic = isPublic1;
  }

  /* ********************************************************************** */
  /*                               Accessors                                */
  /* ********************************************************************** */
  public String getUsername() { return username; }
  public String getPath() { return path; }
  public String getGrantor() { return grantor; }
  public boolean isPublic() { return isPublic; }

  @Override
  public String toString() {return TapisUtils.toString(this);}

  @Override
  public boolean equals(Object o)
  {
    if (o == this) return true;
    // Note: no need to check for o==null since instanceof will handle that case
    if (!(o instanceof UserShareInfo2)) return false;
    var that = (UserShareInfo2) o;
    return (Objects.equals(this.username, that.username) && Objects.equals(this.path, that.path) &&
            Objects.equals(this.grantor, that.grantor) && this.isPublic == that.isPublic);
  }

  @Override
  public int hashCode()
  {
    int retVal = (username == null ? 1 : username.hashCode());
    retVal = 31 * retVal + (path == null ? 0 : path.hashCode());
    retVal = 31 * retVal + (grantor == null ? 0 : grantor.hashCode());
    retVal = 31 * retVal + Boolean.hashCode(isPublic);
    return retVal;
  }
}