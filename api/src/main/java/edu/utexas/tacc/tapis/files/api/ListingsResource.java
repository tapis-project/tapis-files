package edu.utexas.tacc.tapis.files.api;

import edu.utexas.tacc.tapis.files.lib.clients.RemoteFileInfo;

import javax.ws.rs.*;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.SecurityContext;
import java.util.ArrayList;
import java.util.List;

@Path("listings")
@Produces(MediaType.APPLICATION_JSON)
public class ListingsResource {

    @GET
    @Path("{systemId}/{filePath}/")
    public List<RemoteFileInfo> listFiles(@Context SecurityContext sc,
                                          @PathParam("systemId") long systemId,
                                          @PathParam("filePath") String filePath) throws WebApplicationException {
//
//        // TODO: Permissions checks here
////        AuthenticatedUser user = sc.getUserPrincipal();
//
//        try {
//            StorageSystemsDAO dao = new StorageSystemsDAO();
//            StorageSystem system = dao.getStorageSystem("test", "test", 1);
//            S3DataClient client = new S3DataClient(system);
//            return client.ls("/");
//        } catch (IOException e) {
//            throw new WebApplicationException("Could not list files");
//        }
        return new ArrayList();
    }



}
