package edu.utexas.tacc.tapis.files.lib.clients;

import edu.utexas.tacc.tapis.files.lib.utils.LibUtils;
import edu.utexas.tacc.tapis.systems.client.gen.model.AuthnEnum;
import edu.utexas.tacc.tapis.systems.client.gen.model.Credential;
import edu.utexas.tacc.tapis.systems.client.gen.model.TapisSystem;
import org.apache.commons.lang3.StringUtils;
import org.irods.jargon.core.connection.AuthScheme;
import org.irods.jargon.core.connection.IRODSAccount;
import org.irods.jargon.core.exception.JargonException;
import org.irods.jargon.core.pub.DataTransferOperations;
import org.irods.jargon.core.pub.IRODSAccessObjectFactory;
import org.irods.jargon.core.pub.IRODSFileSystem;
import org.irods.jargon.core.pub.io.IRODSFileFactory;

import java.io.IOException;

class IrodsConnection implements AutoCloseable {
    private static final String DEFAULT_RESC = "";
    private IRODSAccessObjectFactory accessObjectFactory;
    private IRODSFileSystem irodsFileSystem;
    private DataTransferOperations dataTransferOperations;
    private IRODSFileFactory irodsFileFactory;
    private IRODSAccount account;
    private final TapisSystem system;
    private final String oboTenant;
    private final String oboUser;
    private final String irodsZone;
    private final String homeDir;

    IrodsConnection(TapisSystem system, String irodsZone, String homeDir, String oboTenant, String oboUser) {
        this.system = system;
        this.oboTenant = oboTenant;
        this.oboUser = oboUser;
        this.homeDir = homeDir;
        this.irodsZone = irodsZone;
    }

    public IRODSFileFactory getFileFactory() throws IOException {
        if (irodsFileFactory != null) {
            return irodsFileFactory;
        }

        IRODSAccessObjectFactory accessObjectFactory = getAccessObjectFactory();
        try {
            irodsFileFactory = accessObjectFactory.getIRODSFileFactory(getIrodsAccount());
            return irodsFileFactory;
        } catch (JargonException ex) {
            String msg = LibUtils.getMsg("FILES_IRODS_ERROR", oboTenant, "", oboTenant, oboUser);
            throw new IOException(msg, ex);
        }
    }

    public DataTransferOperations getTransferOperations() throws IOException {
        if (dataTransferOperations != null) {
            return dataTransferOperations;
        }

        try {
            accessObjectFactory = getAccessObjectFactory();
            dataTransferOperations = accessObjectFactory.getDataTransferOperations(getIrodsAccount());
            return dataTransferOperations;
        } catch (JargonException ex) {
            String msg = LibUtils.getMsg("FILES_IRODS_ERROR", oboTenant, "", oboTenant, oboUser);
            throw new IOException(msg, ex);
        }
    }

    private IRODSAccessObjectFactory getAccessObjectFactory() throws IOException {
        try {
            if (account == null) {
                account = getIrodsAccount();

            }

            if (irodsFileSystem == null) {
                irodsFileSystem = IRODsFileSystemSingleton.getInstance();
            }

            if (accessObjectFactory == null) {
                accessObjectFactory = irodsFileSystem.getIRODSAccessObjectFactory();
            }
        } catch (JargonException ex) {
            String msg = LibUtils.getMsg("FILES_IRODS_ERROR", oboTenant, "", oboTenant, oboUser);
            throw new IOException(msg, ex);
        }

        return accessObjectFactory;
    }

    private IRODSAccount getIrodsAccount() throws JargonException {
        if (account != null) {
            return account;
        }

        String user = null;
        String password = null;

        if (AuthnEnum.PASSWORD.equals(system.getDefaultAuthnMethod())) {
            Credential cred = system.getAuthnCredential();
            if (cred != null) {
                password = cred.getPassword();
                user = system.getEffectiveUserId();
            }
        }

        // Make sure we have a user and password.  Blank passwords are not allowed.
        if (StringUtils.isBlank(user) || StringUtils.isBlank(password)) {
            String msg = LibUtils.getMsg("FILES_IRODS_CREDENTIAL_ERROR", oboTenant, oboUser, system.getId(), system.getDefaultAuthnMethod());
            throw new IllegalArgumentException(msg);
        }

        // getUseProxy returns capital B Boolean.  Use Boolean.TRUE.equals() to handle null propeerly
        if (Boolean.TRUE.equals(system.getUseProxy())) {
            return IRODSAccount.instanceWithProxy(system.getHost(), system.getPort(), oboUser, password,
                    homeDir, irodsZone, DEFAULT_RESC, user, irodsZone);
        } else {
            return IRODSAccount.instance(system.getHost(), system.getPort(), user, password,
                    homeDir, irodsZone, DEFAULT_RESC, AuthScheme.STANDARD);
        }
    }

    @Override
    public void close() {
        if (dataTransferOperations != null) {
            dataTransferOperations.closeSessionAndEatExceptions();
        }

        if (this.irodsFileSystem != null) {
            if(account != null) {
                this.irodsFileSystem.closeAndEatExceptions(account);
            } else {
                this.irodsFileSystem.closeAndEatExceptions();
            }
        }

        if (this.accessObjectFactory != null) {
            this.accessObjectFactory.closeSessionAndEatExceptions();
        }
    }
}
