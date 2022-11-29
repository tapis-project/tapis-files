package edu.utexas.tacc.tapis.files.api.responses;

import edu.utexas.tacc.tapis.files.lib.models.NativeLinuxOpResult;
import edu.utexas.tacc.tapis.sharedapi.responses.RespAbstract;

public final class RespNativeLinuxOpResult extends RespAbstract
{
  public RespNativeLinuxOpResult(NativeLinuxOpResult nlo) { result = nlo;}
  public NativeLinuxOpResult result;
}
