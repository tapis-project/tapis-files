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
  private final Set<String> userSet; // List of users with whom the path is shared.

  /* ********************************************************************** */
  /*                           Constructors                                 */
  /* ********************************************************************** */

  // Default constructor to set defaults. This appears to be needed for when object is created from json using gson.
  public ShareInfo()
  {
    isPublic = false;
    userSet = null;
  }

  public ShareInfo(boolean isPublic1, Set<String> userList1)
  {
    isPublic = isPublic1;
    userSet = userList1;
  }

  /* ********************************************************************** */
  /*                               Accessors                                */
  /* ********************************************************************** */
  public boolean isPublic() { return isPublic; }
  public Set<String> getUserSet() { return (userSet == null) ? null : new HashSet<>(userSet); }

  @Override
  public String toString() {return TapisUtils.toString(this);}
}