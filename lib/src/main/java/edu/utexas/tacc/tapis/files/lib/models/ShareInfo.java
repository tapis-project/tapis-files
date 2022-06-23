package edu.utexas.tacc.tapis.files.lib.models;

import java.util.HashSet;
import java.util.Set;

import edu.utexas.tacc.tapis.shared.utils.TapisUtils;

/**
 * Share information for a file path
 *
 * This class is intended to represent an immutable object.
 * Please keep it immutable.
 */
public final class ShareInfo
{
  private final boolean isPublic; // Indicates if path is shared publicly for all users in the tenant
  private final String isPublicPath; // Path that resulted in specified path being shared publicly
  private final Set<String> userSet; // Set of users with whom the path is shared.
  private final Set<UserShareInfo> userShareInfoSet; // List of additional share information for each user

  /* ********************************************************************** */
  /*                           Constructors                                 */
  /* ********************************************************************** */

  // Default constructor to set defaults. This appears to be needed for when object is created from json using gson.
  public ShareInfo()
  {
    isPublic = false;
    isPublicPath = null;
    userSet = null;
    userShareInfoSet = null;
  }

  public ShareInfo(boolean isPublic1, String isPublicPath1, Set<String> userList1, Set<UserShareInfo> userShareInfoSet1)
  {
    isPublic = isPublic1;
    isPublicPath = isPublicPath1;
    userSet = userList1;
    userShareInfoSet = userShareInfoSet1;
  }

  /* ********************************************************************** */
  /*                               Accessors                                */
  /* ********************************************************************** */
  public boolean isPublic() { return isPublic; }
  public String getIsPublicPath() { return isPublicPath; }
  public Set<String> getUserSet() { return (userSet == null) ? null : new HashSet<>(userSet); }
  public Set<UserShareInfo> getUserShareInfoSet() { return (userShareInfoSet == null) ? null : new HashSet<>(userShareInfoSet); }

  @Override
  public String toString() {return TapisUtils.toString(this);}
}