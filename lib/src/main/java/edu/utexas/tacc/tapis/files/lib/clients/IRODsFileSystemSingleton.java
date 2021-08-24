package edu.utexas.tacc.tapis.files.lib.clients;

import edu.utexas.tacc.tapis.files.lib.utils.Utils;
import org.irods.jargon.core.exception.JargonException;
import org.irods.jargon.core.pub.IRODSFileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;


/**
 * The documentation for the IRODSFileSystem class says to wrap
 * the class in a singleton for lookup later. Should not create one
 * for every request/operation. Here, we create a singleton holder for
 * the connection and use that in the actual IrodsDataClient that implements
 * the operations on the irods filesystem.
 */
public class IRODsFileSystemSingleton {

    private static final Logger log = LoggerFactory.getLogger(IRODsFileSystemSingleton.class);

    private static IRODSFileSystem instance;

    private IRODsFileSystemSingleton() {}

    public static synchronized IRODSFileSystem getInstance() throws IOException {
        if (instance == null) {
            try {
                return IRODSFileSystem.instance();
            } catch (JargonException ex) {
                String msg = Utils.getMsg("FILES_IRODS_SESSION_ERROR");
                throw new IOException(msg, ex);
            }
        }
        return instance;
    }

}
