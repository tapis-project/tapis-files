package edu.utexas.tacc.tapis.files.api.responses;

import edu.utexas.tacc.tapis.files.lib.models.ShareInfo;
import edu.utexas.tacc.tapis.sharedapi.responses.RespAbstract;

public final class RespShareInfo extends RespAbstract
{
    public RespShareInfo(ShareInfo result) { this.result = result;}
    
    public ShareInfo result;
}
