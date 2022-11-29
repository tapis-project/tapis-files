package edu.utexas.tacc.tapis.files.api.responses;

import edu.utexas.tacc.tapis.files.lib.models.TransferTask;
import edu.utexas.tacc.tapis.sharedapi.responses.RespAbstract;

public final class RespTransferTask extends RespAbstract
{
  public RespTransferTask() {}
  public RespTransferTask(TransferTask t) { result = t;}
  public TransferTask result;
}
