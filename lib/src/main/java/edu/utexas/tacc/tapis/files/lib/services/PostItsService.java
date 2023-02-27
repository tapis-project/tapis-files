package edu.utexas.tacc.tapis.files.lib.services;

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

    // Default allowed uses 1
    private static Integer DEFAULT_ALLOWED_USES = Integer.valueOf(1);

    // Default ttl 30 days
    private static Integer DEFAULT_VALID_SECONDS =  Integer.valueOf(2592000);


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

    /**
     * Used to create a special AuthenticatedUser based on the information from a postit.
     * This is used during redeeming a PostIt to act as the AuthenticatedUser for calls
     * that require one - but it will have the owner and tenant of the postit as the
     * OboUser and OboTenant
     */
    class PostItUser extends AuthenticatedUser {
        public PostItUser(PostIt postIt) {
            super(null, null, null, null,
                    postIt.getOwner(), postIt.getTenantId(),
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
            allowedUses = DEFAULT_ALLOWED_USES;
        }
        if(validSeconds == null) {
            validSeconds = DEFAULT_VALID_SECONDS;
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

        return postItsDAO.createPostIt(postIt);
    }

    /**
     * Get a PostIt by Id.  A user must own the PostIt and it must be in the
     * OboTenant or the user must be a tenant admin of the tenant of the PostIt
     * to get the PostIt.
     *
     * @param rUser ResourceRequestUser representing the user that is updating
     *              the PostIt.
     * @param postItId Id of the PostIt to update
     * @return the requested PostIt
     * @throws TapisException
     * @throws ServiceException
     */
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
        String msg = LibUtils.getMsg("POSTIT_NOT_AUTHORIZED", postIt.getTenantId(), postIt.getOwner(),
                postIt.getSystemId(), postItId);
        log.warn(msg);
        throw new ForbiddenException(msg);
    }

    /**
     * List PostIts.  The list will contain all PostIts owned by the OboUser in the OboDomain.  If the
     * OboUser is a tenant admin, the list will contain all PostIts in the domain.
     *
     * @param rUser ResourceRequestUser representing the user that is updating
     *              the PostIt.
     * @return List containing the matching PostIts
     * @throws TapisException
     * @throws ServiceException
     */
    public List<PostIt> listPostIts(ResourceRequestUser rUser) throws TapisException, ServiceException {
        String tenantId = rUser.getOboTenantId();
        String owner = rUser.getOboUserId();

        // If the owner is a tenant admin, don't limit them to only the PostIts
        // that they own.  Tenant admins can see all PostIts in thier tenant
        if(isTenantAdmin(tenantId, owner)) {
            owner = null;
        }

        return postItsDAO.listPostIts(tenantId, owner);
    }

    /**
     * Updates a PostIt by Id.  The allowed uses, and expirationSeconds can be
     * updated.  No other fields of a PostIt can be updated.  A user must own
     * the PostIt and it must be in the OboTenant or the user must be a tenant
     * admin of the tenant of the PostIt to update.
     *
     * @param rUser ResourceRequestUser representing the user that is updating
     *              the PostIt.
     * @param postItId Id of the PostIt to update
     * @param validSeconds number of seconds this PostIt is valid
     * @param allowedUses number of times this PostIt can be redeemed
     *
     * @return the newly updated PostIt
     * @throws TapisException
     * @throws ServiceException
     */
    public PostIt updatePostIt(ResourceRequestUser rUser, String postItId,
                               Integer validSeconds, Integer allowedUses)
            throws TapisException, ServiceException {
        PostIt postIt = postItsDAO.getPostIt(postItId);
        if(!isOwnerOrAdmin(postIt, rUser)) {
            String msg = LibUtils.getMsg("POSTIT_NOT_AUTHORIZED",
                    postIt.getTenantId(), postIt.getOwner(),
                    postIt.getSystemId(), postItId);
            log.warn(msg);
            throw new ForbiddenException(msg);
        }

        if(validSeconds != null) {
            postIt.setExpiration(Instant.now().plus(validSeconds, ChronoUnit.SECONDS));
        }
        if(allowedUses != null) {
            postIt.setAllowedUses(allowedUses);
        }

        PostIt updatedPostIt = postItsDAO.updatePostIt(postIt);

        if(updatedPostIt == null) {
            String msg = LibUtils.getMsg("POSTIT_SERVICE_ERROR_ID", updatedPostIt.getId());
            throw new NotFoundException(msg);
        }

        return updatedPostIt;
    }

    /**
     * Redeem a PostIt by id.  If zip is null, this method will automatically
     * zip directories, and not zip files.  If zip is false for a directory,
     * an Exception will be thrown - directories must always be zipped.  The
     * usage count of the PostIt will be incremented just prior to the download
     * beginning.
     *
     * @param postItId - the Id of the postit to redeem.
     * @param zip true/false/null.  If null, directories will be zipped, but files
     *            will not.  If false and the PostIt points to a directory, an
     *            exception will be thrown.
     * @return PostItRedeemContext containing information related to redeeming.
     * @throws TapisException
     */
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

        ResourceRequestUser rUser = new ResourceRequestUser(new PostItUser(postIt));
        redeemContext.setrUser(rUser);

        String systemId = postIt.getSystemId();
        String path = postIt.getPath();

        // Get system. This requires READ permission.
        TapisSystem tapisSystem = LibUtils.getSystemIfEnabled(rUser, systemsCache, systemId);

        // Determine the target file name to use in ContentDisposition (.zip will get added for zipStream)
        java.nio.file.Path inPath = Paths.get(path);
        java.nio.file.Path  filePath = inPath.getFileName();
        String fileName = (filePath == null) ? "root" : filePath.getFileName().toString();

        // fileOpsService.getFileInfo() will check path permissions, so no need to check in this method.
        FileInfo fileInfo = fileOpsService.getFileInfo(rUser, tapisSystem, path, null, null);
        if (fileInfo == null)
        {
            throw new NotFoundException(LibUtils.getMsgAuthR("FILES_CONT_NO_FILEINFO", rUser, systemId, path));
        }

        if(zip == null) {
            // if no zip preference was supplied, we will zip if it's a directory,
            // and not zip if it's not a directory.
            zip = fileInfo.isDir() ? Boolean.TRUE : Boolean.FALSE;
        } else if (zip.equals(Boolean.FALSE) && (fileInfo.isDir())) {
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
