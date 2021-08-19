package edu.utexas.tacc.tapis.files.lib.clients;

import edu.utexas.tacc.tapis.files.lib.models.FileInfo;
import edu.utexas.tacc.tapis.files.lib.utils.Utils;
import edu.utexas.tacc.tapis.systems.client.gen.model.TapisSystem;
import org.apache.commons.io.FilenameUtils;
import org.irods.jargon.core.connection.AuthScheme;
import org.irods.jargon.core.connection.IRODSAccount;
import org.irods.jargon.core.exception.JargonException;
import org.irods.jargon.core.pub.IRODSAccessObjectFactory;
import org.irods.jargon.core.pub.IRODSFileSystem;
import org.irods.jargon.core.pub.io.IRODSFileFactory;
import org.irods.jargon.core.utils.MiscIRODSUtils;
import org.jetbrains.annotations.NotNull;

import javax.ws.rs.NotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

public class IrodsDataClient implements IRemoteDataClient {

    private final String oboTenantId;
    private final String oboUsername;
    private final TapisSystem system;
    private final String irodsZone;
    private final String homeDir;
    private static final String DEFAULT_RESC = "demoResc";

    public IrodsDataClient(@NotNull String oboTenantId, @NotNull String oboUsername, @NotNull TapisSystem system) {
        this.oboTenantId = oboTenantId;
        this.oboUsername = oboUsername;
        this.system = system;
        Path tmpPath = Paths.get(system.getRootDir());
        this.irodsZone = tmpPath.getRoot().toString();
        this.homeDir = Paths.get(irodsZone, "home", oboUsername).toString();
    }

    @Override
    public String getOboTenant() {
        return oboTenantId;
    }

    @Override
    public String getOboUser() {
        return oboUsername;
    }

    @Override
    public String getSystemId() {
        return system.getId();
    }

    @Override
    public List<FileInfo> ls(@NotNull String remotePath) throws IOException, NotFoundException {
        return null;
    }

    @Override
    public List<FileInfo> ls(@NotNull String remotePath, long limit, long offset) throws IOException, NotFoundException {
        return null;
    }

    @Override
    public void insert(@NotNull String remotePath, @NotNull InputStream fileStream) throws IOException {

    }

    @Override
    public void mkdir(@NotNull String remotePath) throws IOException, NotFoundException {
        String cleanedPath = FilenameUtils.normalize(remotePath);
        IRODSAccessObjectFactory accessObjectFactory;
        IRODSFileSystem irodsFileSystem;
        IRODSAccount account;
        IRODSFileFactory fileFactory;
        try {
           irodsFileSystem = IRODSFileSystem.instance();
           account = IRODSAccount.instance(
                system.getHost(),
                system.getPort(),
                oboUsername,
                system.getAuthnCredential().getPassword(),
                homeDir,
                irodsZone,
                DEFAULT_RESC,
                AuthScheme.STANDARD
            );
           accessObjectFactory = irodsFileSystem.getIRODSAccessObjectFactory();
           fileFactory = accessObjectFactory.getIRODSFileFactory(account);
        } catch (JargonException ex) {
            String msg = Utils.getMsg("FILES_IRODS_ERROR", oboTenantId, "", oboTenantId, oboUsername);
            throw new IOException(msg, ex);
        }




    }

    @Override
    public void move(@NotNull String oldPath, @NotNull String newPath) throws IOException, NotFoundException {

    }

    @Override
    public void copy(@NotNull String currentPath, @NotNull String newPath) throws IOException, NotFoundException {

    }

    @Override
    public void delete(@NotNull String path) throws IOException {

    }

    @Override
    public InputStream getStream(@NotNull String path) throws IOException {
        return null;
    }

    @Override
    public InputStream getBytesByRange(@NotNull String path, long startByte, long count) throws IOException {
        return null;
    }

    @Override
    public void putBytesByRange(String path, InputStream byteStream, long startByte, long endByte) throws IOException {

    }

    @Override
    public void append(@NotNull String path, @NotNull InputStream byteStream) throws IOException {

    }

    @Override
    public void download(String path) throws IOException {

    }
}
