package edu.utexas.tacc.tapis.files.lib.services;

import edu.utexas.tacc.tapis.files.gen.jooq.tables.FilesPostits;
import edu.utexas.tacc.tapis.files.lib.caches.SystemsCache;
import edu.utexas.tacc.tapis.files.lib.caches.TenantAdminCache;
import edu.utexas.tacc.tapis.files.lib.dao.postits.PostItsDAO;
import edu.utexas.tacc.tapis.files.lib.exceptions.ServiceException;
import edu.utexas.tacc.tapis.files.lib.models.FileInfo;
import edu.utexas.tacc.tapis.files.lib.models.PostIt;
import edu.utexas.tacc.tapis.files.lib.utils.LibUtils;
import edu.utexas.tacc.tapis.shared.exceptions.TapisException;
import edu.utexas.tacc.tapis.sharedapi.security.AuthenticatedUser;
import edu.utexas.tacc.tapis.sharedapi.security.ResourceRequestUser;
import edu.utexas.tacc.tapis.systems.client.gen.model.TapisSystem;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.StringUtils;
import org.jooq.Condition;
import org.jvnet.hk2.annotations.Service;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.ws.rs.BadRequestException;
import javax.ws.rs.ForbiddenException;
import javax.ws.rs.NotFoundException;
import javax.ws.rs.core.MediaType;
import java.nio.file.Paths;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;

@Service
public class PostItsService {
    private static final Logger log = LoggerFactory.getLogger(PostItsService.class);
    @Inject
    PostItsDAO postItsDAO;

    @Inject
    FilePermsService permsService;

    @Inject
    FileOpsService fileOpsService;

    @Inject
    SystemsCache systemsCache;

    @Inject
    TenantAdminCache tenantAdminCache;

    // Used to create a special ResourceRequestUser based on the information from a postit
    class PostItUser extends AuthenticatedUser {
        public PostItUser(PostIt postIt) {
            super(null, null, null, null,
                    postIt.getJwtUser(), postIt.getJwtTenantId(),
                    null, null, null);
        }
    }

    /**
     * Creates a new PostIt.  The path must exist, and the OboUser must have permission
     * for the path in the OboTenant.
     *
     * @param systemId Id of the system where the path is located
     * @param path path relative to the root of the system
     * @param validSeconds number of seconds this PostIt will be valid.
     * @param allowedUses number of times the PostIt may be redeemed.
     * @param rUser ResourceRequest user.
     * @return the newly created PostIt.
     * @throws TapisException
     * @throws ServiceException
     */
    public PostIt createPostIt(ResourceRequestUser rUser, String systemId, String path,
                               Integer validSeconds, Integer allowedUses)
            throws TapisException, ServiceException {

        // check for path permissions
        LibUtils.checkPermitted(permsService, rUser.getOboTenantId(),
                rUser.getOboUserId(), systemId, path, FileInfo.Permission.READ);

        // make sure the file exists
        TapisSystem system = LibUtils.getSystemIfEnabled(rUser, systemsCache, systemId);
        FileInfo fileInfo = fileOpsService.getFileInfo(rUser, system, path, null, null);
        if(fileInfo == null) {
            String msg = LibUtils.getMsgAuthR("POSTIT_SERVICE_ERROR", rUser,
                    systemId, path, "Unable to get file info");
            log.error(msg);
            throw new TapisException(msg);
        }

        // apply defaults
        if(allowedUses == null) {
            allowedUses = Integer.valueOf(1);
        }
        if(validSeconds == null) {
            validSeconds = Integer.valueOf(2592000);
        }

        // Set fields that are set by this service.
        PostIt postIt = new PostIt();
        postIt.setSystemId(systemId);
        postIt.setPath(path);
        postIt.setExpiration(Instant.now().plus(validSeconds, ChronoUnit.SECONDS));
        postIt.setAllowedUses(allowedUses);
        postIt.setOwner(rUser.getOboUserId());
        postIt.setTenantId(rUser.getOboTenantId());
        postIt.setJwtUser(rUser.getJwtUserId());
        postIt.setJwtTenantId(rUser.getJwtTenantId());
        postIt.setTimesUsed(0);
        postIt.setCreated(null);
        postIt.setUpdated(null);


        return postItsDAO.createPostIt(postIt);
    }

    public PostIt getPostIt(ResourceRequestUser rUser, String postItId) throws TapisException, ServiceException {
        PostIt postIt = postItsDAO.getPostIt(postItId);

        if(postIt == null) {
            String msg = LibUtils.getMsg("POSTIT_NOT_FOUND", postItId);
            log.warn(msg);
            throw new NotFoundException(msg);
        }

        // Check to see if the OboUser is the owner or is a tenant admin for the postit's tenant
        if(isOwnerOrAdmin(postIt, rUser)) {
            return postIt;
        }

        // If the OboUser doesn't own the postit in the OboTenant, and is not a tenant admin,
        // they are not permitted.
        String msg = LibUtils.getMsg("FILES_NOT_AUTHORIZED", postIt.getTenantId(), postIt.getOwner(),
                postIt.getSystemId(), postIt.getPath());
        log.warn(msg);
        throw new ForbiddenException(msg);
    }

    public List<PostIt> listPostIts(ResourceRequestUser rUser) throws TapisException, ServiceException {
        Condition condition = FilesPostits.FILES_POSTITS.TENANTID.eq(rUser.getOboTenantId());
        condition = condition.and(FilesPostits.FILES_POSTITS.OWNER.eq(rUser.getOboUserId()));
        // TODO:  Add something so that a tenant admin can get all postits for their tenant
        // perhaps in the select criteria for list.
        return postItsDAO.listPostIts(rUser.getOboTenantId(), checkPostIt -> {
            return isOwnerOrAdmin(checkPostIt, rUser);
        });
    }

    public PostIt updatePostIt(ResourceRequestUser rUser, String postItId,
                               Integer validSeconds, Integer allowedUses)
            throws TapisException, ServiceException {
        PostIt postIt = new PostIt();
        postIt.setId(postItId);
        if(validSeconds != null) {
            postIt.setExpiration(Instant.now().plus(validSeconds, ChronoUnit.SECONDS));
        }
        if(allowedUses != null) {
            postIt.setAllowedUses(allowedUses);
        }

        PostIt updatedPostIt = postItsDAO.updatePostIt(postIt);

        if(updatedPostIt == null) {
            String msg = LibUtils.getMsg("POSTIT_SERVICE_ERROR_ID", postIt.getId());
            throw new NotFoundException(msg);
        }

        return updatedPostIt;
    }

    public PostItRedeemContext redeemPostIt(String postItId, Boolean zip)
            throws TapisException {
        PostItRedeemContext redeemContext = new PostItRedeemContext();
        redeemContext.setZip(false);
        PostIt postIt = postItsDAO.getPostIt(postItId);
        if(postIt == null) {
            String msg = LibUtils.getMsg("POSTIT_NOT_FOUND", postItId);
            log.warn(msg);
            throw new NotFoundException(msg);
        }

        String systemId = postIt.getSystemId();
        String path = postIt.getPath();
        ResourceRequestUser rUser = new ResourceRequestUser(new PostItUser(postIt));
        redeemContext.setrUser(rUser);

        // Get system. This requires READ permission.
        TapisSystem tapisSystem = LibUtils.getSystemIfEnabled(rUser, systemsCache, systemId);

        // ---------------------------- Make service calls to start data streaming -------------------------------
        // Note that we do not use try/catch around service calls because exceptions are already either
        //   a WebApplicationException or some other exception handled by the mapper that converts exceptions
        //   to responses (ApiExceptionMapper).

        // Determine the target file name to use in ContentDisposition (.zip will get added for zipStream)
        java.nio.file.Path inPath = Paths.get(path);
        java.nio.file.Path  filePath = inPath.getFileName();
        String fileName = (filePath == null) ? "root" : filePath.getFileName().toString();

        FileInfo fileInfo = fileOpsService.getFileInfo(rUser, tapisSystem, path, null, null);
        if (fileInfo == null)
        {
            throw new NotFoundException(LibUtils.getMsgAuthR("FILES_CONT_NO_FILEINFO", rUser, systemId, path));
        }

        if(zip == null) {
            // if no zip preference was supplied, we will zip if it's a directory,
            // and not zip if it's not a directory.
            zip = fileInfo.isDir() ? Boolean.TRUE : Boolean.FALSE;
        } else if (zip.equals(Boolean.FALSE)) {
            String msg = LibUtils.getMsgAuthR("POSTITS_REDEEM_DIR_NOZIP",
                    rUser, systemId, path);
            log.error(msg);
            throw new BadRequestException(msg);
        }

        redeemContext.setZip(zip.booleanValue());

        // Make a different service call depending on type of response:
        //  - zipStream, byteRangeStream, paginatedStream, fullStream
        if (redeemContext.isZip()) {
            // Send a zip stream. This can handle a path ending in /
            redeemContext.setOutStream(fileOpsService.getZipStream(rUser, tapisSystem, path, null, null));
            String newName = FilenameUtils.removeExtension(fileName) + ".zip";
            redeemContext.setContentDisposition(String.format("attachment; filename=%s", newName));
            redeemContext.setMediaType(MediaType.APPLICATION_OCTET_STREAM);
        } else {
            redeemContext.setOutStream(fileOpsService.getFullStream(rUser, tapisSystem, path, null, null));
            redeemContext.setContentDisposition(String.format("attachment; filename=%s", fileName));
            redeemContext.setMediaType(MediaType.APPLICATION_OCTET_STREAM);
        }

        if(!postItsDAO.incrementUseIfRedeemable(postItId)) {
            String msg = LibUtils.getMsg("POSTIT_SERVICE_ERROR_NOT_REDEEMABLE", postItId);
            log.warn(msg);
            throw new BadRequestException(msg);
        }

        return redeemContext;
    }

    private boolean isOwnerOrAdmin(PostIt postIt, ResourceRequestUser rUser) {
        if(isOwner(postIt, rUser)) {
            return true;
        } else {
            try {
                if(isTenantAdmin(postIt.getTenantId(), rUser.getOboUserId())) {
                    return true;
                }
            } catch (ServiceException e) {
                // in the case of a service exception, we will log the exception, but
                // just assume they are not an admin.
                log.error(e.getMessage(), e);
            }
        }

        return false;
    }

    private boolean isOwner(PostIt postIt, ResourceRequestUser rUser) {
        if(StringUtils.equals(postIt.getTenantId(), rUser.getOboTenantId())) {
            if (StringUtils.equals(postIt.getOwner(), rUser.getOboUserId())) {
                return true;
            }
        }

        return  false;
    }

    private boolean isTenantAdmin(String tenantId, String user) throws ServiceException {
        Boolean admin = tenantAdminCache.checkPerm(tenantId, user);
        if(admin == null) {
            // if no result is returned, assume false;
            admin = Boolean.FALSE;
        }

        return admin.booleanValue();
    }

}
