package edu.utexas.tacc.tapis.files.lib.clients;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import edu.utexas.tacc.tapis.shared.exceptions.TapisClientException;
import edu.utexas.tacc.tapis.systems.client.SystemsClient;
import edu.utexas.tacc.tapis.systems.client.gen.model.TSystem;
import edu.utexas.tacc.tapis.tokens.client.TokensClient;
import okhttp3.OkHttpClient;


public class FakeSystemsService {
    
	private final OkHttpClient client = new OkHttpClient();
	
	// Local logger.
	private static final Logger _log = LoggerFactory.getLogger(FakeSystemsService.class);
	
	
    	public TSystem systemRESTCall(String tenant, String systemId) throws Exception{
    		
    		// Get short term JWT from tokens service
    		_log.debug("Get Short term JWT");
    		
    		//FIXME Change URL passed to TokensClient
    		//      Based on tenants/dev/prod, the URL will change;    
    	    var tokClient = new TokensClient("https://dev.develop.tapis.io");
    	    
    	    String svcJWT;
    	    
    	    _log.debug("Get Service token");
    	    try {svcJWT = tokClient.getSvcToken(tenant, "files" );
    	    } catch (Exception e) {
    	    	throw new Exception("Exception from Tokens service", e);
    	    }
    	    
    	    System.out.println("Got svcJWT: " + svcJWT);
    	    
    	    // Basic check of JWT
    	    if (StringUtils.isBlank(svcJWT)) throw new Exception("Token service returned invalid JWT");
    	    
    	    // Create the client
    	    // Check for URL set as env var
    	    // TODO Obtain the sysURL from Files service environment variables
    	    // TODO String sysURL = Files.getenv("TAPIS_SVC_SYSTEMS_URL");
    	    // FIXME change sysURL
    	    String sysURL = "https://dev.develop.tapis.io";

    	    SystemsClient sysClient;
    	   // if (StringUtils.isBlank(sysURL)) sysURL ="http://localhost:8080";
    	    sysClient = new SystemsClient(sysURL, svcJWT);
    	
    		
    	    TSystem sys1 = null;
    	    try {
			 sys1 = sysClient.getSystemByName(systemId, false);
    	    } catch (TapisClientException e2) {
    	    	// TODO Auto-generated catch block
    	    	e2.printStackTrace();
    	    }
        _log.debug("host: "+ sys1.getHost() + "  effectiveUserId : "+ sys1.getEffectiveUserId() + "  accessMechanism: "+ sys1.getAccessMethod());
       
        
        return sys1;
	  }

	public TSystem getSystemByName(String systemId) {
	  TSystem sys = null;
	  try {
		 sys = systemRESTCall("dev", systemId);
	  } catch (Exception e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	  }

    _log.debug("JSON object string from system service: \n"+ sys);
	  return sys;
	  
 }
	 
}
