package edu.utexas.tacc.tapis.files.api.responses;

import edu.utexas.tacc.tapis.files.api.responses.results.ResultFilesList;
import edu.utexas.tacc.tapis.sharedapi.responses.RespAbstract;

public final class RespFilesList extends RespAbstract {
	
	public RespFilesList(ResultFilesList result) { this.result = result;}
    
    public ResultFilesList result ;
}
