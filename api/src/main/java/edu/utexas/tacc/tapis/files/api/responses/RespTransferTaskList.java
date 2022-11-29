package edu.utexas.tacc.tapis.files.api.responses;

import java.util.List;
import edu.utexas.tacc.tapis.files.lib.models.TransferTask;
import edu.utexas.tacc.tapis.sharedapi.responses.RespAbstract;

/*
  Results from a retrieval of SystemHistory items resources.
 */
public final class RespTransferTaskList extends RespAbstract
{
  public List<TransferTask> result;
  public RespTransferTaskList(List<TransferTask> txfrTaskList)
  {
    result = txfrTaskList;
  }
}
