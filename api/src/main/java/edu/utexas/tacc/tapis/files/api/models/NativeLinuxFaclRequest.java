package edu.utexas.tacc.tapis.files.api.models;

import edu.utexas.tacc.tapis.files.lib.services.FileUtilsService;

/**
 * This class will contain the request to set file ACLs on Linux systems.
 */
public class NativeLinuxFaclRequest {
    /**
     * The file ACL operation requested.
     */
    FileUtilsService.NativeLinuxFaclOperation operation;

    /**
     * The rerecursion method used for setting file ACLs.
     */
    FileUtilsService.NativeLinuxFaclRecursion recursionMethod;

    /**
     * The ACL entry to set.  See linux man page for details.
     * For example on Ubuntu Linux:
     *        [d[efault]:] [u[ser]:]uid [:perms]
     *               Permissions of a named user. Permissions of the file owner if uid is empty.
     *
     *        [d[efault]:] g[roup]:gid [:perms]
     *               Permissions of a named group. Permissions of the owning group if gid is empty.
     *
     *        [d[efault]:] m[ask][:] [:perms]
     *               Effective rights mask
     *
     *        [d[efault]:] o[ther][:] [:perms]
     *               Permissions of others.
     */
    String aclString;

    public NativeLinuxFaclRequest() {
        recursionMethod = FileUtilsService.NativeLinuxFaclRecursion.NONE;
    }

    public FileUtilsService.NativeLinuxFaclOperation getOperation() {
        return operation;
    }

    public void setOperation(FileUtilsService.NativeLinuxFaclOperation operation) {
        this.operation = operation;
    }

    public FileUtilsService.NativeLinuxFaclRecursion getRecursionMethod() {
        return recursionMethod;
    }

    public void setRecursionMethod(FileUtilsService.NativeLinuxFaclRecursion recursionMethod) {
        this.recursionMethod = recursionMethod;
    }

    public String getAclString() {
        return aclString;
    }

    public void setAclString(String aclString) {
        this.aclString = aclString;
    }
}
