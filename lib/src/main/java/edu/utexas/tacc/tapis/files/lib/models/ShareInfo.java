package edu.utexas.tacc.tapis.files.lib.models;

import java.util.ArrayList;
import java.util.List;
import edu.utexas.tacc.tapis.shared.utils.TapisUtils;

/**
 * Share information for a file path
 *
 * This class is intended to represent an immutable object.
 * Please keep it immutable.
 */
public final class ShareInfo
{
  private final boolean isPublic;
  private final List<String> userList;

  /* ********************************************************************** */
  /*                           Constructors                                 */
  /* ********************************************************************** */

  // Default constructor to set defaults. This appears to be needed for when object is created from json using gson.
  public ShareInfo()
  {
    isPublic = false;
    userList = null;
  }

  public ShareInfo(boolean isPublic1, List<String> userList1)
  {
    isPublic = isPublic1;
    userList = userList1;
  }

  /* ********************************************************************** */
  /*                               Accessors                                */
  /* ********************************************************************** */
  public boolean isPublic() { return isPublic; }
  public List<String> getUserList() { return (userList == null) ? null : new ArrayList<>(userList); }

  @Override
  public String toString() {return TapisUtils.toString(this);}
}