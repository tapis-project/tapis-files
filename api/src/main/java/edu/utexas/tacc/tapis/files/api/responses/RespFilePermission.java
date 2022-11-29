package edu.utexas.tacc.tapis.files.api.responses;

import edu.utexas.tacc.tapis.files.lib.models.FilePermission;
import edu.utexas.tacc.tapis.sharedapi.responses.RespAbstract;

public final class RespFilePermission extends RespAbstract
{
  public RespFilePermission(FilePermission perm) { result = perm;}
  public FilePermission result;
}
