package edu.utexas.tacc.tapis.files.lib.clients;

import edu.utexas.tacc.tapis.files.lib.models.FileInfo;
import edu.utexas.tacc.tapis.files.lib.utils.Utils;
import edu.utexas.tacc.tapis.systems.client.gen.model.TapisSystem;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.StringUtils;
import org.irods.jargon.core.connection.AuthScheme;
import org.irods.jargon.core.connection.IRODSAccount;
import org.irods.jargon.core.exception.DataNotFoundException;
import org.irods.jargon.core.exception.FileNotFoundException;
import org.irods.jargon.core.exception.JargonException;
import org.irods.jargon.core.exception.JargonFileOrCollAlreadyExistsException;
import org.irods.jargon.core.packinstr.DataObjInp;
import org.irods.jargon.core.packinstr.TransferOptions;
import org.irods.jargon.core.pub.DataTransferOperations;
import org.irods.jargon.core.pub.IRODSAccessObjectFactory;
import org.irods.jargon.core.pub.IRODSFileSystem;
import org.irods.jargon.core.pub.io.IRODSFile;
import org.irods.jargon.core.pub.io.IRODSFileFactory;
import org.irods.jargon.core.pub.io.IRODSFileInputStream;
import org.irods.jargon.core.pub.io.IRODSFileOutputStream;
import org.irods.jargon.core.pub.io.PackingIrodsInputStream;
import org.irods.jargon.core.transfer.DefaultTransferControlBlock;
import org.irods.jargon.core.transfer.TransferControlBlock;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.NotFoundException;
import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

import static edu.utexas.tacc.tapis.files.lib.services.FileOpsService.MAX_LISTING_SIZE;

public class IrodsDataClient implements IRemoteDataClient {

    private static final Logger log = LoggerFactory.getLogger(IrodsDataClient.class);
    private final String oboTenantId;
    private final String oboUsername;
    private final TapisSystem system;
    private final String irodsZone;
    private final String homeDir;
    private final String rootDir;
    private static final String DEFAULT_RESC = "";
    private static final int MAX_BYTES_PER_CHUNK = 1000000;

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
        long count = Math.min(limit, MAX_LISTING_SIZE);
        long startIdx = Math.max(offset, 0);
        String cleanedPath = FilenameUtils.normalize(remotePath);
        String fullPath = Paths.get("/", rootDir, cleanedPath).toString();
        Path rootDirPath = Paths.get(rootDir);
        IRODSFileFactory fileFactory = getFileFactory();
        try {
            IRODSFile collection = fileFactory.instanceIRODSFile(fullPath);
            // If the listing is just a single file make the listing manually.
            if (collection.isFile()) {
                List<FileInfo> outListing = new ArrayList<>();
                FileInfo fileInfo = new FileInfo();
                fileInfo.setSize(collection.length());
                fileInfo.setType("file");
                fileInfo.setName(collection.getName());
                Path tmpPath = Paths.get(collection.getPath());
                Path relPath = rootDirPath.relativize(tmpPath);
                fileInfo.setPath(relPath.toString());
                fileInfo.setLastModified(Instant.ofEpochSecond(collection.lastModified()));
                outListing.add(fileInfo);
                return outListing;

            }
            List<File> listing = Arrays.asList(collection.listFiles());
            collection.close();
            List<FileInfo> outListing = new ArrayList<>();
            listing.forEach((file) -> {
                Path tmpPath = Paths.get(file.getPath());
                Path relPath = rootDirPath.relativize(tmpPath);
                FileInfo fileInfo = new FileInfo();
                fileInfo.setPath(relPath.toString());
                fileInfo.setName(file.getName());
                fileInfo.setType(file.isDirectory() ? "dir" : "file");
                fileInfo.setSize(file.length());
                try {
                    fileInfo.setMimeType(Files.probeContentType(tmpPath));
                } catch (IOException ignored) {
                }

                fileInfo.setLastModified(Instant.ofEpochMilli(file.lastModified()));
                outListing.add(fileInfo);
            });
            outListing.sort(Comparator.comparing(FileInfo::getName));
            return outListing.stream().skip(startIdx).limit(count).collect(Collectors.toList());
        } catch (JargonException ex) {
            String msg = Utils.getMsg("FILES_IRODS_ERROR", oboTenantId, "", oboTenantId, oboUsername);
            throw new IOException(msg, ex);
        } catch (Exception ex) {
            if (ex.getCause() instanceof FileNotFoundException) {
                throw new NotFoundException();
            }
            else {
                throw ex;
            }
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
        IRODSFileFactory fileFactory = getFileFactory();

        try {
            //Make sure parent path exists first
            IRODSFile parent = fileFactory.instanceIRODSFile(parentDir.toString());
            if (!parent.exists()) {
                Path relativePathtoParent = Paths.get(rootDir).relativize(parentDir);
                mkdir(relativePathtoParent.toString());
            }
            parent.close();

            IRODSFile newFile = fileFactory.instanceIRODSFile(fullPath.toString());
            try (
                fileStream;
                IRODSFileOutputStream outputStream = fileFactory.instanceIRODSFileOutputStream(newFile);
            ) {
                fileStream.transferTo(outputStream);
            } catch (JargonException ex) {
                String msg = Utils.getMsg("FILES_IRODS_ERROR", oboTenantId, "", oboTenantId, oboUsername);
                throw new IOException(msg, ex);
            }
            finally {
                newFile.close();
            }
        } catch (JargonException ex) {
            String msg = Utils.getMsg("FILES_IRODS_ERROR", oboTenantId, "", oboTenantId, oboUsername);
            throw new IOException(msg, ex);
        }
    }


    @Override
    public void mkdir(@NotNull String remotePath) throws IOException, NotFoundException {
        if (StringUtils.isEmpty(remotePath)) return;
        Path cleanedRelativePath = cleanAndRelativize(remotePath);
        IRODSFileFactory fileFactory = getFileFactory();
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
                newCollection.close();
            }

        } catch (JargonException ex) {
            String msg = Utils.getMsg("FILES_IRODS_ERROR", oboTenantId, "", oboTenantId, oboUsername);
            throw new IOException(msg, ex);
        }
    }

    @Override
    public void move(@NotNull String oldPath, @NotNull String newPath) throws IOException, NotFoundException {
        Path cleanedRelativeOldPath = cleanAndRelativize(oldPath);
        Path cleanedAbsoluteOldPath = Paths.get(rootDir, cleanedRelativeOldPath.toString());
        Path cleanedRelativeNewPath = cleanAndRelativize(newPath);
        Path cleanedAbsoluteNewPath = Paths.get(rootDir, cleanedRelativeNewPath.toString());
        DataTransferOperations transferOperations = getTransferOperations();
        IRODSFileFactory fileFactory = getFileFactory();
        Path parentDirTarget = cleanedAbsoluteNewPath.getParent();
        try {
            //Make sure parent path exists first
            IRODSFile parent = fileFactory.instanceIRODSFile(parentDirTarget.toString());
            if (!parent.exists()) {
                Path relativePathtoParent = Paths.get(rootDir).relativize(parentDirTarget);
                mkdir(relativePathtoParent.toString());
            }
            parent.close();
            transferOperations.move(cleanedAbsoluteOldPath.toString(), cleanedAbsoluteNewPath.toString());
        } catch (JargonFileOrCollAlreadyExistsException ex) {
            String msg = Utils.getMsg("FILES_IRODS_MOVE_ERROR_DEST_EXISTS", oboTenantId, oboUsername, cleanedRelativeNewPath.toString());
            throw new IOException(msg, ex);
        } catch (JargonException ex) {
            String msg = Utils.getMsg("FILES_IRODS_ERROR", oboTenantId, "", oboTenantId, oboUsername, system.getId());
            throw new IOException(msg, ex);
        }
    }

    @Override
    public void copy(@NotNull String sourcePath, @NotNull String destPath) throws IOException, NotFoundException {
        Path cleanedRelativeSourcePath = cleanAndRelativize(sourcePath);
        Path cleanedAbsoluteSourcePath = Paths.get(rootDir, cleanedRelativeSourcePath.toString());
        Path cleanedRelativeDestPath = cleanAndRelativize(destPath);
        Path cleanedAbsoluteDestPath = Paths.get(rootDir, cleanedRelativeDestPath.toString());
        DataTransferOperations transferOperations = getTransferOperations();
        IRODSFileFactory fileFactory = getFileFactory();
        Path parentDirTarget = cleanedAbsoluteDestPath.getParent();
        try {
            //Make sure parent path exists first
            IRODSFile parent = fileFactory.instanceIRODSFile(parentDirTarget.toString());
            if (!parent.exists()) {
                Path relativePathtoParent = Paths.get(rootDir).relativize(parentDirTarget);
                mkdir(relativePathtoParent.toString());
            }
            parent.close();

            IRODSFile source = fileFactory.instanceIRODSFile(cleanedAbsoluteSourcePath.toString());
            IRODSFile destination = fileFactory.instanceIRODSFile(cleanedAbsoluteDestPath.toString());
            TransferOptions transferOptions = new TransferOptions();
            transferOptions.setComputeAndVerifyChecksumAfterTransfer(true);
            transferOptions.setForceOption(TransferOptions.ForceOption.USE_FORCE);
            TransferControlBlock transferControlBlock = DefaultTransferControlBlock.instance();
            transferControlBlock.setTransferOptions(transferOptions);
            transferOperations.copy(
                source,
                destination,
                null,
                transferControlBlock
            );
        } catch (DataNotFoundException ex) {
            String msg = Utils.getMsg("FILES_IRODS_FILE_NOT_FOUND_ERROR", system.getId(), oboTenantId, oboUsername, cleanedRelativeDestPath.toString());
            throw new NotFoundException(msg, ex);
        } catch (JargonFileOrCollAlreadyExistsException ex) {
            String msg = Utils.getMsg("FILES_IRODS_MOVE_ERROR_DEST_EXISTS", oboTenantId, oboUsername, cleanedRelativeDestPath.toString());
            throw new IOException(msg, ex);
        } catch (JargonException ex) {
            String msg = Utils.getMsg("FILES_IRODS_ERROR", oboTenantId, "", oboTenantId, oboUsername, system.getId());
            throw new IOException(msg, ex);
        }
    }

    @Override
    public void delete(@NotNull String remotePath) throws IOException {
        if (StringUtils.isEmpty(remotePath)) remotePath="/";
        Path cleanedRelativePath = cleanAndRelativize(remotePath);
        Path cleanedAbsolutePath = Paths.get(rootDir, cleanedRelativePath.toString());
        Path rootDirPath = Paths.get(rootDir);
        IRODSFileFactory fileFactory = getFileFactory();
        try {
            IRODSFile collection = fileFactory.instanceIRODSFile(cleanedAbsolutePath.toString());
            if (collection.isFile()) {
                collection.delete();
            } else {
                for (File file : collection.listFiles()) {
                    IRODSFile tmp = fileFactory.instanceIRODSFile(file.getPath());
                    tmp.deleteWithForceOption();
                }
                //Can't delete above the rootDir
                if (!rootDirPath.equals(cleanedAbsolutePath)) {
                    collection.delete();
                }
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
        IRODSFileFactory fileFactory = getFileFactory();
        try {
            InputStream stream = new PackingIrodsInputStream(fileFactory.instanceIRODSFileInputStream(cleanedAbsolutePath.toString()));
            return stream;
        } catch (JargonException ex) {
            String msg = Utils.getMsg("FILES_IRODS_ERROR", oboTenantId, "", oboTenantId, oboUsername);
            throw new IOException(msg, ex);
        }
    }


    /**
     *
     * @param path path to file
     * @param startByte position of first byte to return
     * @param count Number of bytes returned
     * @return InputStream of the chunk of the file
     * @throws IOException error getting stream
     */
    @Override
    public InputStream getBytesByRange(@NotNull String path, long startByte, long count) throws IOException {
        if (count > MAX_BYTES_PER_CHUNK) {
            String msg = Utils.getMsg("FILES_MAX_BYTES_ERROR", MAX_BYTES_PER_CHUNK);
            throw new IllegalArgumentException(msg);
        }
        startByte = Math.max(startByte, 0);
        count = Math.max(count, 0);
        byte[] bytes = new byte[(int) count];
        try (InputStream stream = getStream(path)) {
            stream.skip(startByte);
            int counter = 0;
            int bytesRead = stream.read(bytes);
            if (bytesRead > 0) {
                bytes = Arrays.copyOfRange(bytes, 0, bytesRead);
            } else {
                bytes = new byte[0];
            }
        };
        return new ByteArrayInputStream(bytes);
    }

    @Override
    public void putBytesByRange(String path, InputStream byteStream, long startByte, long endByte) throws IOException {

    }

    @Override
    public void append(@NotNull String path, @NotNull InputStream byteStream) throws IOException {

    }

    /**
     * Cleans and ensures that the path is relative
     * @param remotePath path relative to rootDir
     * @return Path object that has any leading slashes removed and cleaned.
     */
    private Path cleanAndRelativize(String remotePath) {
        remotePath = StringUtils.removeStart(remotePath, "/");
        String cleanedPath = FilenameUtils.normalize(remotePath);
        return Paths.get(cleanedPath);
    }

    private IRODSFileFactory getFileFactory() throws IOException {
        IRODSAccessObjectFactory accessObjectFactory;
        IRODSFileSystem irodsFileSystem;
        IRODSAccount account;
        IRODSFileFactory fileFactory;
        try {
            irodsFileSystem = IRODsFileSystemSingleton.getInstance();
            account = IRODSAccount.instance(
                system.getHost(),
                system.getPort(),
                system.getAuthnCredential().getAccessKey(),
                system.getAuthnCredential().getAccessSecret(),
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
            irodsFileSystem = IRODsFileSystemSingleton.getInstance();
            account = IRODSAccount.instance(
                system.getHost(),
                system.getPort(),
                system.getAuthnCredential().getAccessKey(),
                system.getAuthnCredential().getAccessSecret(),
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
