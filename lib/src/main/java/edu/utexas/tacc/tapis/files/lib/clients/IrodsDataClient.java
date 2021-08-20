package edu.utexas.tacc.tapis.files.lib.clients;

import edu.utexas.tacc.tapis.files.lib.models.FileInfo;
import edu.utexas.tacc.tapis.files.lib.utils.Utils;
import edu.utexas.tacc.tapis.systems.client.gen.model.TapisSystem;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.StringUtils;
import org.irods.jargon.core.connection.AuthScheme;
import org.irods.jargon.core.connection.IRODSAccount;
import org.irods.jargon.core.exception.JargonException;
import org.irods.jargon.core.pub.DataTransferOperations;
import org.irods.jargon.core.pub.IRODSAccessObjectFactory;
import org.irods.jargon.core.pub.IRODSFileSystem;
import org.irods.jargon.core.pub.io.FileIOOperations;
import org.irods.jargon.core.pub.io.IRODSFile;
import org.irods.jargon.core.pub.io.IRODSFileFactory;
import org.irods.jargon.core.pub.io.IRODSFileInputStream;
import org.irods.jargon.core.pub.io.IRODSFileOutputStream;
import org.irods.jargon.core.utils.MiscIRODSUtils;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.NotFoundException;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class IrodsDataClient implements IRemoteDataClient {

    private static final Logger log = LoggerFactory.getLogger(IrodsDataClient.class);
    private final String oboTenantId;
    private final String oboUsername;
    private final TapisSystem system;
    private final String irodsZone;
    private final String homeDir;
    private final String rootDir;
    private static final String DEFAULT_RESC = "demoResc";

    public IrodsDataClient(@NotNull String oboTenantId, @NotNull String oboUsername, @NotNull TapisSystem system) {
        this.oboTenantId = oboTenantId;
        this.oboUsername = oboUsername;
        this.system = system;
        this.rootDir = system.getRootDir();
        Path tmpPath = Paths.get(system.getRootDir());
        this.irodsZone = tmpPath.subpath(0, 1).toString();
        this.homeDir = Paths.get("/",irodsZone, "home", oboUsername).toString();
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
        return this.ls(remotePath, 1000, 0);
    }

    @Override
    public List<FileInfo> ls(@NotNull String remotePath, long limit, long offset) throws IOException, NotFoundException {
        String cleanedPath = FilenameUtils.normalize(remotePath);
        String fullPath = Paths.get("/", rootDir, cleanedPath).toString();
        Path rootDirPath = Paths.get(rootDir);
        IRODSFileFactory fileFactory = getFactory();

        try {
            IRODSFile collection= fileFactory.instanceIRODSFile(fullPath);
            List<File> listing = Arrays.asList(collection.listFiles());
            List<FileInfo> outListing = new ArrayList<>();
            listing.forEach((file)->{
                Path tmpPath = Paths.get(file.getPath());
                Path relPath = rootDirPath.relativize(tmpPath);
                FileInfo fileInfo = new FileInfo();
                fileInfo.setPath(relPath.toString());
                fileInfo.setName(file.getName());
                fileInfo.setType(file.isDirectory() ? "dir" : "file");
                fileInfo.setSize(file.length());
                try {
                    fileInfo.setMimeType(Files.probeContentType(tmpPath));
                } catch (IOException ignored) {}

                fileInfo.setLastModified(Instant.ofEpochSecond(file.lastModified()));
                outListing.add(fileInfo);
            });
            return outListing;
        } catch (JargonException ex) {
            String msg = Utils.getMsg("FILES_IRODS_ERROR", oboTenantId, "", oboTenantId, oboUsername);
            throw new IOException(msg, ex);
        }
    }

    /**
     *
     * @param remotePath Always relative to rootDir
     * @param fileStream InputStream to send to irods
     * @throws IOException if auth failed or insert failed
     */
    @Override
    public void insert(@NotNull String remotePath, @NotNull InputStream fileStream) throws IOException {
        Path cleanedPath = cleanAndRelativize(remotePath);
        Path fullPath = Paths.get("/", rootDir, cleanedPath.toString());
        Path parentDir = fullPath.getParent();
        IRODSFileFactory fileFactory = getFactory();

        try {
            //Make sure parent path exists first
            IRODSFile parent = fileFactory.instanceIRODSFile(parentDir.toString());
            if (!parent.exists()) {
                Path relativePathtoParent = Paths.get(rootDir).relativize(parentDir);
                mkdir(relativePathtoParent.toString());
            }

            IRODSFile newFile = fileFactory.instanceIRODSFile(fullPath.toString());
            try (
                fileStream;
                IRODSFileOutputStream outputStream = fileFactory.instanceIRODSFileOutputStream(newFile);
            ) {
                fileStream.transferTo(outputStream);
            }
        } catch (JargonException ex) {
            String msg = Utils.getMsg("FILES_IRODS_ERROR", oboTenantId, "", oboTenantId, oboUsername);
            throw new IOException(msg, ex);
        }
    }

    private void mkdirRecurse(@NotNull String remotePath) throws IOException {

    }

    @Override
    public void mkdir(@NotNull String remotePath) throws IOException, NotFoundException {
        if (StringUtils.isEmpty(remotePath)) return;
        Path cleanedRelativePath = cleanAndRelativize(remotePath);
        IRODSFileFactory fileFactory = getFactory();
        try {
            List<Path> partialPaths = new ArrayList<>();
            Path tmpPath = Paths.get("/");
            for(Path part: cleanedRelativePath) {
                Path newPath = tmpPath.resolve(part);
                partialPaths.add(newPath);
                tmpPath = newPath;
            }
            for (Path part: partialPaths) {
                Path tmp = Paths.get(rootDir, part.toString());
                IRODSFile newCollection = fileFactory.instanceIRODSFile(tmp.toString());
                if (!newCollection.exists()) {
                    newCollection.mkdir();
                }
            }

        } catch (JargonException ex) {
            String msg = Utils.getMsg("FILES_IRODS_ERROR", oboTenantId, "", oboTenantId, oboUsername);
            throw new IOException(msg, ex);
        }
    }

    @Override
    public void move(@NotNull String oldPath, @NotNull String newPath) throws IOException, NotFoundException {
        Path cleanedRelativeOldPath = cleanAndRelativize(oldPath);
        Path cleanedAbsoluteoldPathPath = Paths.get(rootDir, cleanedRelativeOldPath.toString());


        IRODSFileFactory fileFactory = getFactory();
        try {
            IRODSFile collection = fileFactory.instanceIRODSFile(cleanedAbsoluteoldPathPath.toString());
            collection.
        } catch (JargonException ex) {
            String msg = Utils.getMsg("FILES_IRODS_ERROR", oboTenantId, "", oboTenantId, oboUsername);
            throw new IOException(msg, ex);
        }
    }

    @Override
    public void copy(@NotNull String currentPath, @NotNull String newPath) throws IOException, NotFoundException {
        try {
            DataTransferOperations dataTransferOperations = getTransferOperations();
            dataTransferOperations.
        }
    }

    @Override
    public void delete(@NotNull String remotePath) throws IOException {
        if (StringUtils.isEmpty(remotePath)) return;
        Path cleanedRelativePath = cleanAndRelativize(remotePath);
        Path cleanedAbsolutePath = Paths.get(rootDir, cleanedRelativePath.toString());
        IRODSFileFactory fileFactory = getFactory();
        try {
            IRODSFile collection = fileFactory.instanceIRODSFile(cleanedAbsolutePath.toString());
            for (File file: collection.listFiles()) {
                IRODSFile tmp = fileFactory.instanceIRODSFile(file.getPath());
                tmp.delete();
            }
        } catch (JargonException ex) {
            String msg = Utils.getMsg("FILES_IRODS_ERROR", oboTenantId, "", oboTenantId, oboUsername);
            throw new IOException(msg, ex);
        }
    }

    @Override
    public InputStream getStream(@NotNull String remotePath) throws IOException {
        Path cleanedRelativePath = cleanAndRelativize(remotePath);
        Path cleanedAbsolutePath = Paths.get(rootDir, cleanedRelativePath.toString());
        IRODSFileFactory fileFactory = getFactory();
        try {
            IRODSFileInputStream stream = fileFactory.instanceIRODSFileInputStream(cleanedAbsolutePath.toString());
            return stream;
        } catch (JargonException ex) {
            String msg = Utils.getMsg("FILES_IRODS_ERROR", oboTenantId, "", oboTenantId, oboUsername);
            throw new IOException(msg, ex);
        }
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

    private Path cleanAndRelativize(String remotePath) {
        remotePath = StringUtils.removeStart(remotePath, "/");
        String cleanedPath = FilenameUtils.normalize(remotePath);
        return Paths.get(cleanedPath);
    }

    private IRODSFileFactory getFactory() throws IOException {
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
            return fileFactory;
        } catch (JargonException ex) {
            String msg = Utils.getMsg("FILES_IRODS_ERROR", oboTenantId, "", oboTenantId, oboUsername);
            throw new IOException(msg, ex);
        }
    }

    private DataTransferOperations getTransferOperations() throws IOException {
        IRODSFileSystem irodsFileSystem;
        IRODSAccount account;
        IRODSAccessObjectFactory accessObjectFactory;

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
            DataTransferOperations ops = accessObjectFactory.getDataTransferOperations(account);
            return ops;
        } catch (JargonException ex) {
            String msg = Utils.getMsg("FILES_IRODS_ERROR", oboTenantId, "", oboTenantId, oboUsername);
            throw new IOException(msg, ex);
        }
    }
}
