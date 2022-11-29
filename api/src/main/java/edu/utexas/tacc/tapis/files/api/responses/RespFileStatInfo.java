package edu.utexas.tacc.tapis.files.api.responses;

import edu.utexas.tacc.tapis.files.lib.models.FileStatInfo;
import edu.utexas.tacc.tapis.sharedapi.responses.RespAbstract;

public final class RespFileStatInfo extends RespAbstract
{
  public RespFileStatInfo(FileStatInfo fsi) { result = fsi;}
  public FileStatInfo result;
}
