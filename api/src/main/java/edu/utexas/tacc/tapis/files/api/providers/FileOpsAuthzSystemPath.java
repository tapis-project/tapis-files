package edu.utexas.tacc.tapis.files.api.providers;
import java.io.IOException;

import javax.inject.Inject;
import javax.ws.rs.BadRequestException;
import javax.ws.rs.NotAuthorizedException;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerRequestFilter;
import javax.ws.rs.container.ResourceInfo;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MultivaluedMap;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.utexas.tacc.tapis.security.client.SKClient;
import edu.utexas.tacc.tapis.shared.exceptions.TapisClientException;
import edu.utexas.tacc.tapis.sharedapi.security.AuthenticatedUser;
import edu.utexas.tacc.tapis.tokens.client.TokensClient;

@FileOpsAuthorization
public class FileOpsAuthzSystemPath implements ContainerRequestFilter {

    private Logger log = LoggerFactory.getLogger(FileOpsAuthzSystemPath.class);
    private AuthenticatedUser user;
    // PERMSPEC is "files:tenant:r,rw,*:systemId:path
    private String PERMSPEC = "files:%s:%s:%s:%s";
    
    //TODO move config file?
    //get info from Tenant Manager??
    private String SK_BASE_URL = "https://dev.develop.tapis.io/v3";
    private String TOKEN_BASE_URL = "https://dev.develop.tapis.io";

    //@Inject private SKClient skClient;
    private SKClient skClient = new SKClient(SK_BASE_URL,null); 
  
    @Context
    private ResourceInfo resourceInfo;

    @Override
    public void filter(ContainerRequestContext requestContext) throws IOException {

        FileOpsAuthorization requiredPerms = resourceInfo.getResourceMethod().getAnnotation(FileOpsAuthorization.class);

        user = (AuthenticatedUser) requestContext.getSecurityContext().getUserPrincipal();
        String username = user.getName();
        String tenantId = user.getTenantId();
        MultivaluedMap<String, String> params = requestContext.getUriInfo().getPathParameters();
        log.info("Parameters:" + params.toString());
        String path = params.getFirst("path");
        String systemId = params.getFirst("systemId");

        //TODO: Empty path should be allowed, defaults to rootDir
        if (path.isEmpty() || systemId.isEmpty()) {
            throw new BadRequestException("bad request");
        }
        String permSpec = String.format(PERMSPEC, tenantId, requiredPerms.permsRequired().getLabel(), systemId, path);
        //skClient.setBasePath(path);
        skClient.addDefaultHeader("X-Tapis-Token", getSvcJWT());
        skClient.addDefaultHeader("X-Tapis-Tenant", tenantId);
        skClient.addDefaultHeader("X-Tapis-User", username);
        //skClient.setUserAgent("SKClient");
        log.debug("permSpec: " + permSpec);
        log.debug("username: " + username);
        
        try {
            boolean isPermitted = skClient.isPermitted(username, permSpec);
            if (!isPermitted) {
                throw new NotAuthorizedException("Not authorized to access this file/folder");
            }
            log.debug("user is permitted to list files");
        } catch (TapisClientException e) {
            log.error("FileOpsAuthzSystemPath", e);
            throw new WebApplicationException("Something went wrong...");
        }



    }
    
    /* ********************************************* */
    /*             Private Method                    */
    /* ********************************************* */
    String getSvcJWT(){
        // Use the tokens service to get a user token
        String tokensBaseURL = TOKEN_BASE_URL;
        var tokClient = new TokensClient(tokensBaseURL);
        String svcJWT = "";
        try {
           svcJWT = tokClient.getSvcToken("dev", "files");
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        System.out.println("Got svcJWT: " + svcJWT);
        // Basic check of JWT
        if (StringUtils.isBlank(svcJWT))
        {
          System.out.println("Token service returned invalid JWT");
          System.exit(1);
        }
        return svcJWT;
        }
}

