package edu.utexas.tacc.tapis.files.lib.clients;

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
import javax.ws.rs.BadRequestException;
import javax.ws.rs.NotFoundException;

import edu.utexas.tacc.tapis.systems.client.gen.model.AuthnEnum;
import edu.utexas.tacc.tapis.systems.client.gen.model.Credential;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.StringUtils;
import org.irods.jargon.core.connection.AuthScheme;
import org.irods.jargon.core.connection.IRODSAccount;
import org.irods.jargon.core.exception.DataNotFoundException;
import org.irods.jargon.core.exception.FileNotFoundException;
import org.irods.jargon.core.exception.JargonException;
import org.irods.jargon.core.exception.JargonFileOrCollAlreadyExistsException;
import org.irods.jargon.core.exception.JargonRuntimeException;
import org.irods.jargon.core.packinstr.TransferOptions;
import org.irods.jargon.core.pub.DataTransferOperations;
import org.irods.jargon.core.pub.IRODSAccessObjectFactory;
import org.irods.jargon.core.pub.IRODSFileSystem;
import org.irods.jargon.core.pub.io.IRODSFile;
import org.irods.jargon.core.pub.io.IRODSFileFactory;
import org.irods.jargon.core.pub.io.IRODSFileOutputStream;
import org.irods.jargon.core.pub.io.PackingIrodsInputStream;
import org.irods.jargon.core.transfer.DefaultTransferControlBlock;
import org.irods.jargon.core.transfer.TransferControlBlock;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.utexas.tacc.tapis.shared.utils.PathUtils;
import edu.utexas.tacc.tapis.files.lib.models.FileInfo;
import edu.utexas.tacc.tapis.files.lib.utils.LibUtils;
import edu.utexas.tacc.tapis.systems.client.gen.model.SystemTypeEnum;
import edu.utexas.tacc.tapis.systems.client.gen.model.TapisSystem;

/**
 * This class provides remoteDataClient file operations for IRODS systems.
 * All path parameters as inputs to methods are assumed to be relative to the rootDir
 * of the system unless otherwise specified.
 */
public class IrodsDataClient implements IRemoteDataClient
{
  private static final Logger log = LoggerFactory.getLogger(IrodsDataClient.class);
  private final String oboTenant;
  private final String oboUser;
  private final String impersonationId;
  private final TapisSystem system;
  private final String systemId;
  private final String irodsZone;
  private final String homeDir;
  private final String rootDir;
  private final String host;
  private final int port;
  private static final String DEFAULT_RESC = "";
  private static final int MAX_BYTES_PER_CHUNK = 1000000;
  public static final long MAX_LISTING_SIZE = Long.MAX_VALUE;

  public IrodsDataClient(@NotNull String oboTenant1, @NotNull String oboUser1, @NotNull TapisSystem system1) throws IOException {
      this(oboTenant1, oboUser1, null, system1);
  }

  public IrodsDataClient(@NotNull String oboTenant1, @NotNull String oboUser1, @NotNull String impersonationId1, @NotNull TapisSystem system1) throws IOException
  {
    oboTenant = oboTenant1;
    oboUser = oboUser1;
    impersonationId = impersonationId1;
    system = system1;
    systemId = system1.getId();
    // Make sure we have a valid rootDir that is not null and does not have extra whitespace
    rootDir = (StringUtils.isBlank(system1.getRootDir())) ? "" :  system1.getRootDir();
    host = system1.getHost();
    port = system.getPort() == null ? -1 : system.getPort();
    Path tmpPath = Paths.get(rootDir);
    if(tmpPath.getNameCount() < 1) {
        String msg = LibUtils.getMsg("FILES_IRODS_ZONE_ERROR", oboTenant, oboUser, system.getId(), system.getRootDir());
        log.error(msg);
        throw new IOException(msg);
    }
    irodsZone = tmpPath.subpath(0, 1).toString();
    homeDir = Paths.get("/", irodsZone, "home", system.getEffectiveUserId()).toString();
  }

  @Override
  public String getOboTenant() {
    return oboTenant;
  }
  @Override
  public String getOboUser() { return oboUser; }
  @Override
  public String getSystemId() { return systemId; }
  @Override
  public SystemTypeEnum getSystemType() { return system.getSystemType(); }
  @Override
  public TapisSystem getSystem() { return system; }

  @Override
  public List<FileInfo> ls(@NotNull String path) throws IOException, NotFoundException
  {
    return this.ls(path, 1000, 0);
  }

  @Override
  public List<FileInfo> ls(@NotNull String path, long limit, long offset) throws IOException, NotFoundException
  {
    long count = Math.min(limit, MAX_LISTING_SIZE);
    long startIdx = Math.max(offset, 0);
    String cleanedPath = FilenameUtils.normalize(path);
    String fullPath = Paths.get("/", rootDir, cleanedPath).toString();
    Path rootDirPath = Paths.get(rootDir);
    IRODSFileFactory fileFactory = getFileFactory();
    try
    {
      IRODSFile collection = fileFactory.instanceIRODSFile(fullPath);
      // If the listing is just a single file make the listing manually.
      if (collection.isFile()) {
        List<FileInfo> outListing = new ArrayList<>();
        FileInfo fileInfo = new FileInfo();
        fileInfo.setSize(collection.length());
        fileInfo.setType(FileInfo.FileType.FILE);
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
        fileInfo.setType(getFileInfoType(collection));
        if(file.isDirectory()) {
            fileInfo.setType(FileInfo.FileType.DIR);
        } else if (file.isFile()) {
            fileInfo.setType(FileInfo.FileType.FILE);
        } else {
            fileInfo.setType(FileInfo.FileType.UNKNOWN);
        }
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
      String msg = LibUtils.getMsg("FILES_IRODS_ERROR", oboTenant, "", oboTenant, oboUser);
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
     * @param path Always relative to rootDir
     * @param fileStream InputStream to send to irods
     * @throws IOException if auth failed or insert failed
     */
    @Override
    public void upload(@NotNull String path, @NotNull InputStream fileStream) throws IOException {
        Path cleanedPath = cleanAndRelativize(path);
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
                String msg = LibUtils.getMsg("FILES_IRODS_ERROR", oboTenant, "", oboTenant, oboUser);
                throw new IOException(msg, ex);
            }
            finally {
                newFile.close();
            }
        } catch (JargonException ex) {
            String msg = LibUtils.getMsg("FILES_IRODS_ERROR", oboTenant, "", oboTenant, oboUser);
            throw new IOException(msg, ex);
        }
    }


    @Override
    public void mkdir(@NotNull String path) throws BadRequestException, IOException {
        if (StringUtils.isEmpty(path)) return;
        Path cleanedRelativePath = cleanAndRelativize(path);
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
                if(newCollection.isFile()) {
                    String msg = LibUtils.getMsg("FILES_CLIENT_IRODS_MKDIR_FILE", oboTenant, oboUser, systemId,
                            system.getEffectiveUserId(), host, tmpPath.toString(), tmp.toString());
                    throw new BadRequestException(msg);
                }
                if (!newCollection.exists()) {
                    if(!newCollection.mkdir()) {
                        String msg = LibUtils.getMsg("FILES_IRODS_DIRECTORY_NOT_CREATED", oboTenant, oboUser,
                                systemId, system.getEffectiveUserId(), host, tmpPath.toString(), tmp.toString());
                        throw new IOException(msg);
                    }
                }
                newCollection.close();
            }

        } catch (JargonException ex) {
            String msg = LibUtils.getMsg("FILES_IRODS_ERROR", oboTenant, "", oboTenant, oboUser);
            throw new IOException(msg, ex);
        }
    }

    @Override
    public void move(@NotNull String srcPath, @NotNull String dstPath) throws IOException, NotFoundException
    {
        Path cleanedRelativeOldPath = cleanAndRelativize(srcPath);
        Path cleanedAbsoluteOldPath = Paths.get(rootDir, cleanedRelativeOldPath.toString());
        Path cleanedRelativeNewPath = cleanAndRelativize(dstPath);
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
            String msg = LibUtils.getMsg("FILES_IRODS_MOVE_ERROR_DEST_EXISTS", oboTenant, oboUser, cleanedRelativeNewPath.toString());
            throw new IOException(msg, ex);
        } catch (JargonException ex) {
            String msg = LibUtils.getMsg("FILES_IRODS_ERROR", oboTenant, "", oboTenant, oboUser, systemId);
            throw new IOException(msg, ex);
        }
    }

    @Override
    public void copy(@NotNull String srcPath, @NotNull String dstPath) throws IOException, NotFoundException {
        Path cleanedRelativeSourcePath = cleanAndRelativize(srcPath);
        Path cleanedAbsoluteSourcePath = Paths.get(rootDir, cleanedRelativeSourcePath.toString());
        Path cleanedRelativeDestPath = cleanAndRelativize(dstPath);
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
            String msg = LibUtils.getMsg("FILES_IRODS_FILE_NOT_FOUND_ERROR", systemId, oboTenant, oboUser, cleanedRelativeDestPath.toString());
            throw new NotFoundException(msg, ex);
        } catch (JargonFileOrCollAlreadyExistsException ex) {
            String msg = LibUtils.getMsg("FILES_IRODS_MOVE_ERROR_DEST_EXISTS", oboTenant, oboUser, cleanedRelativeDestPath.toString());
            throw new IOException(msg, ex);
        } catch (JargonException ex) {
            String msg = LibUtils.getMsg("FILES_IRODS_ERROR", oboTenant, "", oboTenant, oboUser, systemId);
            throw new IOException(msg, ex);
        }
    }

    @Override
    public void delete(@NotNull String remotePath) throws NotFoundException, IOException {
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
        } catch (JargonRuntimeException ex) {
            // the real exception is wrapped in a JargonRuntimeException
            Throwable cause = ex.getCause();
            if(cause != null && FileNotFoundException.class.isAssignableFrom(cause.getClass())) {
                String msg = LibUtils.getMsg("FILES_CLIENT_IRODS_NOT_FOUND", oboTenant,
                        oboUser, systemId, system.getEffectiveUserId(), host, rootDir, remotePath);
                throw new NotFoundException(msg);
            }
        } catch (JargonException ex) {
            String msg = LibUtils.getMsg("FILES_IRODS_ERROR", oboTenant, "", oboTenant, oboUser);
            throw new IOException(msg, ex);
        }
    }

  @Override
  public FileInfo getFileInfo(@NotNull String path, boolean followLinks) throws IOException
  {
    FileInfo fileInfo = null;
    if (StringUtils.isEmpty(path)) path="/";
    Path cleanedRelativePath = cleanAndRelativize(path);
    Path cleanedAbsolutePath = Paths.get(rootDir, cleanedRelativePath.toString());
    Path rootDirPath = Paths.get(rootDir);
    IRODSFileFactory fileFactory = getFileFactory();
    try
    {
      // Get collection. If nothing there return null
      IRODSFile collection = fileFactory.instanceIRODSFile(cleanedAbsolutePath.toString());
      boolean pathExists = collection.exists();
      if (!pathExists) return null;

      fileInfo = new FileInfo();
      fileInfo.setSize(collection.length());
      fileInfo.setName(collection.getName());
      Path tmpPath = Paths.get(collection.getPath());
      Path relPath = rootDirPath.relativize(tmpPath);
      fileInfo.setPath(StringUtils.removeStart(relPath.toString(), "/"));
      fileInfo.setUrl(PathUtils.getTapisUrlFromPath(fileInfo.getPath(), systemId));
      fileInfo.setLastModified(Instant.ofEpochSecond(collection.lastModified()));
      fileInfo.setType(getFileInfoType(collection));
    }
    catch (JargonException ex)
    {
      String msg = LibUtils.getMsg("FILES_IRODS_ERROR", oboTenant, "", oboTenant, oboUser);
      throw new IOException(msg, ex);
    }
    catch (Exception ex)
    {
      // If it is a not found exception return null, else it is not a jargon exception so re-throw it
      if (ex.getCause() instanceof FileNotFoundException) fileInfo = null;
      else throw ex;
    }
    return fileInfo;
  }

  @Override
  public InputStream getStream(@NotNull String remotePath) throws IOException
  {
    Path cleanedRelativePath = cleanAndRelativize(remotePath);
    Path cleanedAbsolutePath = Paths.get(rootDir, cleanedRelativePath.toString());
    IRODSFileFactory fileFactory = getFileFactory();
    try
    {
      return new PackingIrodsInputStream(fileFactory.instanceIRODSFileInputStream(cleanedAbsolutePath.toString()));
    }
    catch (JargonException ex)
    {
      if (ex.getMessage().contains("FileNotFound"))
      {
        String msg = LibUtils.getMsg("FILES_IRODS_PATH_NOT_FOUND", oboTenant, oboUser, systemId, cleanedAbsolutePath.toString());
        throw new NotFoundException(msg);
      }
      else
      {
        String msg = LibUtils.getMsg("FILES_IRODS_ERROR", oboTenant, "", oboTenant, oboUser);
        throw new IOException(msg, ex);
      }
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
            String msg = LibUtils.getMsg("FILES_IRODS_MAX_BYTES_ERROR", MAX_BYTES_PER_CHUNK);
            throw new NotFoundException(msg);
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

  /**
   * Cleans and ensures that the path is relative
   * @param remotePath path relative to rootDir
   * @return Path object that has any leading slashes removed and cleaned.
   */
  private Path cleanAndRelativize(String remotePath)
  {
    remotePath = StringUtils.removeStart(remotePath, "/");
    String cleanedPath = FilenameUtils.normalize(remotePath);
    return Paths.get(cleanedPath);
  }

  private IRODSFileFactory getFileFactory() throws IOException
  {
    IRODSAccessObjectFactory accessObjectFactory;
    IRODSFileSystem irodsFileSystem;
    IRODSAccount account;
    IRODSFileFactory fileFactory;
    try
    {
      irodsFileSystem = IRODsFileSystemSingleton.getInstance();
      account = getIrodsAccount();
      accessObjectFactory = irodsFileSystem.getIRODSAccessObjectFactory();
      fileFactory = accessObjectFactory.getIRODSFileFactory(account);
      return fileFactory;
    }
    catch (JargonException ex)
    {
      String msg = LibUtils.getMsg("FILES_IRODS_ERROR", oboTenant, "", oboTenant, oboUser);
      throw new IOException(msg, ex);
    }
  }

  private DataTransferOperations getTransferOperations() throws IOException
  {
    IRODSFileSystem irodsFileSystem;
    IRODSAccount account;
    IRODSAccessObjectFactory accessObjectFactory;
    try
    {
      irodsFileSystem = IRODsFileSystemSingleton.getInstance();
      account = getIrodsAccount();
      accessObjectFactory = irodsFileSystem.getIRODSAccessObjectFactory();
      DataTransferOperations ops = accessObjectFactory.getDataTransferOperations(account);
      return ops;
    }
    catch (JargonException ex)
    {
      String msg = LibUtils.getMsg("FILES_IRODS_ERROR", oboTenant, "", oboTenant, oboUser);
      throw new IOException(msg, ex);
    }
  }

  FileInfo.FileType getFileInfoType(IRODSFile collection) {
      if(collection.isFile()) {
          return FileInfo.FileType.FILE;
      } else if (collection.isDirectory()) {
          return FileInfo.FileType.DIR;
      } else {
          return FileInfo.FileType.UNKNOWN;
      }
  }

  private IRODSAccount getIrodsAccount() throws JargonException {
      String user = null;
      String password = null;

      if(AuthnEnum.PASSWORD.equals(system.getDefaultAuthnMethod())) {
          Credential cred = system.getAuthnCredential();
          if(cred != null) {
              password = cred.getPassword();
              user = system.getEffectiveUserId();
          }
      }

      // Make sure we have a user and password.  Blank passwords are not allowed.
      if(StringUtils.isBlank(user) || StringUtils.isBlank(password)) {
          String msg = LibUtils.getMsg("FILES_IRODS_CREDENTIAL_ERROR", oboTenant, oboUser, system.getId(), system.getDefaultAuthnMethod());
          throw new IllegalArgumentException(msg);
      }

      // getUseProxy returns capital B Boolean.  Use Boolean.TRUE.equals() to handle null propeerly
      if(Boolean.TRUE.equals(system.getUseProxy()))  {
          return IRODSAccount.instanceWithProxy(host, port, getProxiedOboUser(), password,
                  homeDir, irodsZone, DEFAULT_RESC, user, irodsZone);
      } else {
          return IRODSAccount.instance(host, port, user, password,
                  homeDir, irodsZone, DEFAULT_RESC, AuthScheme.STANDARD);
      }
  }

  // TODO:  Ask Steve ... Is this safe?  Is it ok to assume that if there is something in impersonationId I should use it?
  private String getProxiedOboUser() {
      if (StringUtils.isBlank(impersonationId))
          return oboUser;
      else {
          return impersonationId;
      }
  }
}
