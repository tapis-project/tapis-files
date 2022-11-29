package edu.utexas.tacc.tapis.files.api.responses;

import java.util.List;
import edu.utexas.tacc.tapis.files.lib.models.FileInfo;
import edu.utexas.tacc.tapis.sharedapi.responses.RespAbstract;

/*
  Results from a retrieval of SystemHistory items resources.
 */
public final class RespFileInfoList extends RespAbstract
{
  public List<FileInfo> result;
  public RespFileInfoList(List<FileInfo> l)
  {
    result = l;
  }
}