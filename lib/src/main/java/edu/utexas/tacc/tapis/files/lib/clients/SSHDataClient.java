package edu.utexas.tacc.tapis.files.lib.clients;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import javax.ws.rs.BadRequestException;
import javax.ws.rs.ForbiddenException;
import javax.ws.rs.NotAuthorizedException;
import javax.ws.rs.NotFoundException;
import javax.ws.rs.NotSupportedException;

import edu.utexas.tacc.tapis.files.lib.caches.SystemsCache;
import edu.utexas.tacc.tapis.files.lib.exceptions.ServiceException;
import edu.utexas.tacc.tapis.files.lib.models.AclEntry;
import edu.utexas.tacc.tapis.files.lib.services.FileOpsService;
import edu.utexas.tacc.tapis.files.lib.services.FileUtilsService;
import edu.utexas.tacc.tapis.shared.exceptions.recoverable.TapisRecoverableException;
import edu.utexas.tacc.tapis.shared.exceptions.recoverable.TapisSSHAuthException;
import edu.utexas.tacc.tapis.shared.ssh.SshSessionPool;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.sshd.sftp.client.SftpClient.Attributes;
import org.apache.sshd.sftp.client.SftpClient.DirEntry;
import org.apache.sshd.sftp.common.SftpConstants;
import org.apache.sshd.sftp.common.SftpException;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.utexas.tacc.tapis.files.lib.models.FileInfo;
import edu.utexas.tacc.tapis.files.lib.models.FileStatInfo;
import edu.utexas.tacc.tapis.files.lib.models.NativeLinuxOpResult;
import edu.utexas.tacc.tapis.files.lib.utils.LibUtils;
import edu.utexas.tacc.tapis.shared.exceptions.TapisException;
import edu.utexas.tacc.tapis.shared.ssh.apache.SSHExecChannel;
import edu.utexas.tacc.tapis.shared.ssh.apache.SSHSftpClient;
import edu.utexas.tacc.tapis.shared.utils.PathUtils;
import edu.utexas.tacc.tapis.systems.client.gen.model.SystemTypeEnum;
import edu.utexas.tacc.tapis.systems.client.gen.model.TapisSystem;
import static edu.utexas.tacc.tapis.files.lib.services.FileOpsService.MAX_LISTING_SIZE;

/**
 * This class provides remoteDataClient file operations for SSH systems.
 * All path parameters as inputs to methods are assumed to be relative to the rootDir
 * of the system unless otherwise specified.
 */
public class SSHDataClient implements ISSHDataClient
{
  private static final int MAX_PERMS_INT = Integer.parseInt("777", 8);
  // SFTP client throws IOException containing this string if a path does not exist.
  private static final String NO_SUCH_FILE = "no such file";
  private static final int MAX_STDOUT_SIZE = 1000;
  private static final int MAX_STDERR_SIZE = 1000;
  private static final Duration DEFAULT_SESSION_WAIT = Duration.ofMinutes(1);

  private final Logger log = LoggerFactory.getLogger(SSHDataClient.class);

  private final String oboTenant;
  private final String oboUser;

  private final String host;
  private final String effectiveUserId;
  private final String rootDir;
  private final String systemId;
  private final TapisSystem system;
  private final SystemsCache systemsCache;
  private final String impersonationId;
  private final String sharedCtxGrantor;

  @Override
  public String getOboTenant() { return oboTenant; }
  @Override
  public String getOboUser() { return oboUser; }
  @Override
  public String getSystemId() { return systemId; }
  @Override
  public SystemTypeEnum getSystemType() { return system.getSystemType(); }
  @Override
  public TapisSystem getSystem() {
    return getSystemForConnection();
  }

  // Username must start with letter/underscore, contain alphanumeric or _ or -, have at most 32 characters
  //   and may end with $
  private static final Pattern USER_REGEX = Pattern.compile("^[a-z_]([a-z0-9_-]{0,31}|[a-z0-9_-]{0,30}\\$)$");

  public SSHDataClient(@NotNull String oboTenant, @NotNull String oboUser, @NotNull TapisSystem system,
                       @NotNull SystemsCache systemsCache, String impersonationId, String sharedCtxGrantor)
  {
    this.oboTenant = oboTenant;
    this.oboUser = oboUser;
    this.rootDir = PathUtils.getAbsolutePath(system.getRootDir(), "/").toString();
    this.host = system.getHost();
    this.effectiveUserId = system.getEffectiveUserId();
    this.system = system;
    this.systemId = system.getId();
    this.systemsCache = systemsCache;
    this.impersonationId = impersonationId;
    this.sharedCtxGrantor = sharedCtxGrantor;
  }

  public List<FileInfo> ls(@NotNull String path) throws IOException, NotFoundException
  {
    return ls(path, MAX_LISTING_SIZE, 0);
  }

  public List<FileInfo> ls(@NotNull String path, long limit, long offset) throws IOException, NotFoundException {
    return ls(path, limit, offset, null);
  }

  /**
   * Return file listing on path using sftpClient
   *
   * @param path - Path to file or directory relative to the system rootDir
   * @param limit - Max number of items to return
   * @param offset - offset
   * @param pattern - Wildcard (glob) pattern or regex used to filter results.  Regex must be prefixed by "regex:".
   *                Only results with file names that match the regex will be returned.
   * @return list of FileInfo objects
   * @throws IOException       Generally a network error
   * @throws NotFoundException No file at target
   */
  @Override
  public List<FileInfo> ls(@NotNull String path, long limit, long offset, String pattern) throws IOException, NotFoundException
  {
    String opName = "ls";
    long count = Math.min(limit, MAX_LISTING_SIZE);
    long startIdx = Math.max(offset, 0);
    List<FileInfo> filesList = new ArrayList<>();
    List<DirEntry> dirEntries = new ArrayList<>();
    // Get path relative to system rootDir and protect against ../..
    String relPathStr = PathUtils.getRelativePath(path).toString();
    Path absolutePath = PathUtils.getAbsolutePath(rootDir, relPathStr);
    boolean isDirectory = false;

    try(var sessionHolder = borrowAutoCloseableSftpClient(DEFAULT_SESSION_WAIT, true))
    {
      Attributes attributes = sessionHolder.getSession().stat(absolutePath.toString());
      if (attributes.isDirectory())
      {
        // we need to keep track of if this is a directory or not
        isDirectory = true;
        Iterable<DirEntry> tmp = sessionHolder.getSession().readDir(absolutePath.toString());
        tmp.forEach(dirEntries::add);
      }
      else
      {
        DirEntry entry = new DirEntry(relPathStr, relPathStr, attributes);
        dirEntries.add(entry);
      }
    } catch (RuntimeException e) {
      // we have to catch runtime exception here because tmp above is an sftp class the implements iterable, and
      // it wraps all excetpions in a runtime exception for some reason.
      if(e.getCause() != null) {
        if(e.getCause() instanceof IOException) {
          handleSftpException(e, opName, relPathStr);
        }
      }
      throw e;
    } catch (IOException e) {
      handleSftpException(e, opName, relPathStr);
      String msg = LibUtils.getMsg("FILES_CLIENT_SSH_OP_ERR1", oboTenant, oboUser, "ls", systemId, effectiveUserId, host, relPathStr, e.getMessage());
      throw new IOException(msg, e);
    }

    // For each entry in the fileList received, get the fileInfo object
    for (DirEntry entry: dirEntries)
    {
      // Get the file attributes
      Attributes attrs = entry.getAttributes();
      FileInfo fileInfo = new FileInfo();
      // Ignore filename . and ..
      if (entry.getFilename().equals(".") || entry.getFilename().equals(".."))
      {
        continue;
      }
      Path entryPath = Paths.get(entry.getFilename());
      fileInfo.setName(entryPath.getFileName().toString());
      fileInfo.setLastModified(attrs.getModifyTime().toInstant());
      fileInfo.setSize(attrs.getSize());

      //Try to determine the Mimetype
      Path tmpPath = Paths.get(entry.getFilename());
      fileInfo.setMimeType(Files.probeContentType(tmpPath));
      fileInfo.setType(getFileInfoType(attrs));
      fileInfo.setOwner(String.valueOf(attrs.getUserId()));
      fileInfo.setGroup(String.valueOf(attrs.getGroupId()));
      fileInfo.setNativePermissions(FileStatInfo.getPermsFromInt(attrs.getPermissions()));


      // Path should be relative to rootDir.  If the path we are listing is a directory,
      // all we need to do is pre-pend the relative path string because all of the
      // entries will be relative to the directory we were listing.  But, if the
      // path we are listing (i.e. the path passed in) is a file, then we will already
      // have the path relative to the root, so just remove any leading "/" and we
      // are good.
      if (!isDirectory) {
        String thePath = StringUtils.removeStart(entryPath.toString(), "/");
        fileInfo.setPath(thePath);
      } else {
        fileInfo.setPath(Paths.get(relPathStr, entryPath.toString()).toString());
      }
      filesList.add(fileInfo);
    }
    filesList.sort(Comparator.comparing(FileInfo::getName));


    if(StringUtils.isBlank(pattern))  {
      return filesList.stream().skip(startIdx).limit(count).collect(Collectors.toList());
    }

    final boolean isRegEx = (StringUtils.startsWithIgnoreCase(pattern, IRemoteDataClient.REGEX_PREFIX)) ? true : false;
    final String patternOnly = isRegEx ? pattern.replaceFirst("(?i)regex:", "") : pattern;
    final Pattern compiledPattern = isRegEx ? Pattern.compile(patternOnly) : null;

    return filesList.stream().filter((fileInfo) -> {
      if(isRegEx) {
        return compiledPattern.matcher(fileInfo.getName()).find();
      } else {
        return FilenameUtils.wildcardMatch(fileInfo.getName(), patternOnly);
      }

    }).skip(startIdx).limit(count).collect(Collectors.toList());
  }

  /**
   * Create a directory using sftpClient
   * Directories in path will be created as necessary.
   *
   * @param path Normalized path relative to system rootDir
   * @throws IOException Generally a network error
   */
  @Override
  public void mkdir(@NotNull String path) throws IOException, BadRequestException
  {
    path = FilenameUtils.normalize(path);
    Path remote = Paths.get(rootDir, path);
    Path rootDirPath = Paths.get(rootDir);
    Path relativePath = rootDirPath.relativize(remote);
    String remotePathStr = remote.toString();
    // Walk the path parts creating directories as we go
    Path tmpPath = Paths.get(rootDir);
    StringBuilder partRelativePathSB = new StringBuilder();
    for (Path part : relativePath)
    {
      tmpPath = tmpPath.resolve(part);
      String tmpPathStr = tmpPath.toString();
      partRelativePathSB.append(part).append('/');
      // Do a stat to see if path already exists and is a dir or a file.
      // If it does not exist or exists and is a directory then all is good, if it exists and is a file it is an error
      try
      {
        FileStatInfo statInfo = getStatInfo(partRelativePathSB.toString(), true);
        if (statInfo.isDir()) continue;
        else
        {
          String msg = LibUtils.getMsg("FILES_CLIENT_SSH_MKDIR_FILE", oboTenant, oboUser, systemId,
                                       effectiveUserId, host, tmpPathStr, remotePathStr);
          throw new BadRequestException(msg);
        }
      }
      catch (NotFoundException e) { /* Not found is good, it means mkdir should not throw an exception */ }

      // Get the sftpClient so we can perform operations
      try(var sessionHolder = borrowAutoCloseableSftpClient(DEFAULT_SESSION_WAIT, true)) {
        sessionHolder.getSession().mkdir(tmpPathStr); }
      catch (SftpException e)
      {
        handleSftpException(e, "mkdir", tmpPathStr);
      }
    }
  }

  @Override
  public void upload(@NotNull String path, @NotNull InputStream fileStream) throws IOException
  {
    createFile(path, fileStream);
  }

  /**
   * Move oldPath to newPath using sftpClient
   * If newPath is an existing directory then oldPath will be moved into the directory newPath.
   *
   * @param srcPath current location
   * @param dstPath desired location
   * @throws IOException Network errors generally
   * @throws NotFoundException No file found
   */
  @Override
  public void move(@NotNull String srcPath, @NotNull String dstPath) throws IOException, NotFoundException {
    move(srcPath, dstPath, FileOpsService.MoveCopyOperation.MOVE);
  }

  /**
   * Move oldPath to newPath using sftpClient
   * If newPath is an existing directory then oldPath will be moved into the directory newPath.
   *
   * @param srcPath current location
   * @param dstPath desired location
   * @throws IOException Network errors generally
   * @throws NotFoundException No file found
   */
  @Override
  public NativeLinuxOpResult dtnMove(@NotNull String srcPath, @NotNull String dstPath, FileOpsService.MoveCopyOperation op) throws IOException, NotFoundException {
    return move(srcPath, dstPath, op);
  }

  private NativeLinuxOpResult move(@NotNull String srcPath, @NotNull String dstPath, FileOpsService.MoveCopyOperation op) throws IOException, NotFoundException
  {
    // Get paths relative to system rootDir and protect against ../..
    String relOldPathStr = PathUtils.getRelativePath(srcPath).toString();
    String relNewPathStr = PathUtils.getRelativePath(dstPath).toString();
    Path absoluteOldPath = PathUtils.getAbsolutePath(rootDir, relOldPathStr);
    Path absoluteNewPath = PathUtils.getAbsolutePath(rootDir, relNewPathStr);
    Path targetParentPath = absoluteNewPath.getParent();

    //This will throw a NotFoundException if source is not there
    ls(relOldPathStr);


    // Construct and run linux commands to create the target dir and do the copy
    int retCode = 0;

    StringBuilder sb = new StringBuilder();
    ByteArrayOutputStream stdOut = new ByteArrayOutputStream();
    ByteArrayOutputStream stdErr = new ByteArrayOutputStream();
    try(var sessionHolder = borrowAutoCloseableExecChannel(DEFAULT_SESSION_WAIT, true))
    {
      // Set command to make the destination directory and copy the file.
      sb.append("mkdir -p ");
      sb.append(safelySingleQuoteString(targetParentPath.toString()));
      sb.append(";mv ");
      sb.append(safelySingleQuoteString(absoluteOldPath.toString()));
      if(FileOpsService.MoveCopyOperation.SERVICE_MOVE_DIRECTORY_CONTENTS.equals(op)) {
        sb.append("/*");
      }
      sb.append(" ");
      sb.append(safelySingleQuoteString(absoluteNewPath.toString()));
      retCode = sessionHolder.getSession().execute(sb.toString(), stdOut, stdErr, false);
      if(retCode != 0) {
        String partialStdOut = new String(ArrayUtils.subarray(stdOut.toByteArray(), 0, MAX_STDOUT_SIZE));
        String partialStdErr = new String(ArrayUtils.subarray(stdErr.toByteArray(), 0, MAX_STDERR_SIZE));
        String msg = LibUtils.getMsg("FILES_CLIENT_SSH_CMD_ERR", oboTenant, oboUser, "copy", systemId,
                effectiveUserId, host, relOldPathStr, relNewPathStr, retCode, partialStdOut, partialStdErr);
        log.error(msg);
        throw new IOException(msg);
      }
    }
    catch (TapisException e)
    {
      String msg = LibUtils.getMsg("FILES_CLIENT_SSH_OP_ERR2", oboTenant, oboUser, "move", systemId, effectiveUserId, host, srcPath, dstPath, e.getMessage());
      if(!isDtnMove(op)) {
        throw new IOException(msg, e);
      }
    }

    return new NativeLinuxOpResult(sb.toString(), retCode, String.valueOf(stdOut), String.valueOf(stdErr));
  }

  private boolean isDtnMove(FileOpsService.MoveCopyOperation op) {
    if((FileOpsService.MoveCopyOperation.SERVICE_MOVE_FILE_OR_DIRECTORY.equals(op)) ||
            (FileOpsService.MoveCopyOperation.SERVICE_MOVE_DIRECTORY_CONTENTS.equals(op))) {
      return true;
    }

    return false;
  }

  /**
   * Copy oldPath to newPath using sshExecChannel to run linux commands
   * TODO/TBD If newPath is an existing directory then oldPath will be copied into the directory newPath?
   *          what about cp -R or similar?
   *
   * @param srcPath current location
   * @param dstPath desired location
   * @throws IOException Network errors generally
   * @throws NotFoundException No file found
   */
  @Override
  public void copy(@NotNull String srcPath, @NotNull String dstPath) throws IOException, NotFoundException
  {
    // Get paths relative to system rootDir and protect against ../..
    String relOldPathStr = PathUtils.getRelativePath(srcPath).toString();
    String relNewPathStr = PathUtils.getRelativePath(dstPath).toString();
    Path absoluteOldPath = PathUtils.getAbsolutePath(rootDir, relOldPathStr);
    Path absoluteNewPath = PathUtils.getAbsolutePath(rootDir, relNewPathStr);
    Path targetParentPath = absoluteNewPath.getParent();

    //This will throw a NotFoundException if source is not there
    ls(relOldPathStr);

    // Construct and run linux commands to create the target dir and do the copy
    int retCode = 0;
    try(var sessionHolder = borrowAutoCloseableExecChannel(DEFAULT_SESSION_WAIT, true)) {
      // Set command to make the destination directory and copy the file.
      StringBuilder sb = new StringBuilder();
      sb.append("mkdir -p ");
      sb.append(safelySingleQuoteString(targetParentPath.toString()));
      sb.append(";cp -r ");
      sb.append(safelySingleQuoteString(absoluteOldPath.toString()));
      sb.append(" ");
      sb.append(safelySingleQuoteString(absoluteNewPath.toString()));
      ByteArrayOutputStream stdOut = new ByteArrayOutputStream();
      ByteArrayOutputStream stdErr = new ByteArrayOutputStream();
      retCode = sessionHolder.getSession().execute(sb.toString(), stdOut, stdErr, false);
      if(retCode != 0) {
        String partialStdOut = new String(ArrayUtils.subarray(stdOut.toByteArray(), 0, MAX_STDOUT_SIZE));
        String partialStdErr = new String(ArrayUtils.subarray(stdErr.toByteArray(), 0, MAX_STDERR_SIZE));
        String msg = LibUtils.getMsg("FILES_CLIENT_SSH_CMD_ERR", oboTenant, oboUser, "copy", systemId,
                effectiveUserId, host, relOldPathStr, relNewPathStr, retCode, partialStdOut, partialStdErr);
        log.error(msg);
        throw new IOException(msg);
      }
    }
    catch (TapisException e) {
      if (e.getMessage().toLowerCase().contains(NO_SUCH_FILE)) {
        String msg = LibUtils.getMsg("FILES_CLIENT_SSH_NOT_FOUND", oboTenant, oboUser, systemId, effectiveUserId, host, rootDir, relOldPathStr);
        throw new NotFoundException(msg);
      } else {
        String msg = LibUtils.getMsg("FILES_CLIENT_SSH_OP_ERR2", oboTenant, oboUser, "copy", systemId, effectiveUserId, host, relOldPathStr, relNewPathStr, e.getMessage());
        throw new IOException(msg, e);
      }
    }
  }

  /**
   * Delete a file or directory using sftpClient
   *
   * @param path - Path to file or directory relative to the system rootDir
   * @throws IOException Generally a network error
   */
  @Override
  public void delete(@NotNull String path) throws IOException, NotFoundException, NotSupportedException
  {
    // Get path relative to system rootDir and protect against ../..
    String relativePathStr = PathUtils.getRelativePath(path).toString();
    try(var sessionHolder = borrowAutoCloseableSftpClient(DEFAULT_SESSION_WAIT, true)) {
      recursiveDelete(sessionHolder.getSession(), relativePathStr);
    } catch (IOException e) {
      handleSftpException(e, "delete", path);
      String msg = LibUtils.getMsg("FILES_CLIENT_SSH_OP_ERR1", oboTenant, oboUser, "delete", systemId, effectiveUserId, host, relativePathStr, e.getMessage());
      throw new IOException(msg, e);
    }
  }

  /**
   * Get info for file/dir or object
   *
   * @param path - Path to file or directory relative to the system rootDir
   * @throws IOException Generally a network error
   */
  @Override
  public FileInfo getFileInfo(@NotNull String path, boolean followLinks) throws IOException, NotFoundException
  {
    try(var sessionHolder = borrowAutoCloseableSftpClient(DEFAULT_SESSION_WAIT, true)) {
      return getFileInfo(sessionHolder.getSession(), path, followLinks);
    }
  }

  /**
   * Stream data from file using sftpClient
   *
   * @param path - Path to file or directory relative to the system rootDir
   * @return data stream
   * @throws IOException Generally a network error
   * @throws NotFoundException No file at target
   */
  @Override
  public InputStream getStream(@NotNull String path) throws IOException
  {
    Path absPath = PathUtils.getAbsolutePath(rootDir, path);
    SshSessionPool.PooledSshSession<SSHSftpClient> sftpClient = null;
    try
    {
      sftpClient = borrowAutoCloseableSftpClient(DEFAULT_SESSION_WAIT, true);
      InputStream inputStream = sftpClient.getSession().read(absPath.toString());
      // TapisSSHInputStream closes the sftp connection after reading completes
      return new TapisSSHInputStream(inputStream, sftpClient);
    }
    catch (IOException e)
    {
      if(sftpClient != null) {
        sftpClient.close();
      }
      if (e.getMessage().toLowerCase().contains(NO_SUCH_FILE))
      {
        String msg = LibUtils.getMsg("FILES_CLIENT_SSH_NOT_FOUND", oboTenant, oboUser, systemId, effectiveUserId, host, rootDir, path);
        throw new NotFoundException(msg);
      }
      else
      {
        String msg = LibUtils.getMsg("FILES_CLIENT_SSH_OP_ERR1", oboTenant, oboUser, "getStream", systemId, effectiveUserId, host,
                                  path, e.getMessage());
        log.error(msg, e);
        throw new IOException(msg, e);
      }
    }
  }

  @Override
  public InputStream getBytesByRange(@NotNull String path, long startByte, long count) throws IOException
  {
    Path absPath = PathUtils.getAbsolutePath(rootDir, path);
    try(var sessionHolder = borrowAutoCloseableExecChannel(DEFAULT_SESSION_WAIT, true)) {
      //TODO: This should use Piped streams
      String command = String.format("dd if=%s ibs=1 skip=%s count=%s", absPath, startByte, count);
      ByteArrayOutputStream out = new ByteArrayOutputStream();
      ByteArrayOutputStream outErr = new ByteArrayOutputStream();
      sessionHolder.getSession().execute(command, out, outErr, false);
      return new ByteArrayInputStream(out.toByteArray());
    }
    catch (TapisException e)
    {
      //IMPORTANT: Have to return the channel here until we implement PipedInput/PipedOutput streams.
      if (e.getMessage().toLowerCase().contains(NO_SUCH_FILE))
      {
        String msg = LibUtils.getMsg("FILES_CLIENT_SSH_NOT_FOUND", oboTenant, oboUser, systemId, effectiveUserId, host, rootDir, path);
        throw new NotFoundException(msg);
      }
      else
      {
        String msg = LibUtils.getMsg("FILES_CLIENT_SSH_OP_ERR1", oboTenant, oboUser, "getBytesByRange", systemId, effectiveUserId, host, path, e.getMessage());
        log.error(msg, e);
        throw new IOException(msg, e);
      }
    }
  }

  // ------------------------------
  // Native Linux Utility Methods
  // ------------------------------

  /**
   * Returns the statInfo result for a remotePath using sftpClient
   *
   * @param path - path to check
   * @return statInfo result
   * @throws IOException       Generally a network error
   * @throws NotFoundException No file at target
   */
  @Override
  public FileStatInfo getStatInfo(@NotNull String path, boolean followLinks)
          throws IOException, NotFoundException
  {
    FileStatInfo statInfo;
    String opName = followLinks ? "lstat" : "stat";
    // Path should have already been normalized and checked but for safety and security do it
    //   again here. FilenameUtils.normalize() is expected to protect against escaping via ../..
    // Get path relative to system rootDir and protect against ../..
    String relativePathStr = PathUtils.getRelativePath(path).toString();
    String absolutePathStr = PathUtils.getAbsolutePath(rootDir, relativePathStr).toString();
    Attributes sftpAttrs;
    try(var sessionHolder = borrowAutoCloseableSftpClient(DEFAULT_SESSION_WAIT, true)) {
      // If path is a symbolic link then stat gives info for the link target, lstat gives info for the link
      sftpAttrs = followLinks ? sessionHolder.getSession().stat(absolutePathStr) : sessionHolder.getSession().lstat(absolutePathStr);
    } catch (IOException e) {
      handleSftpException(e, opName, path);
      String msg = LibUtils.getMsg("FILES_CLIENT_SSH_OP_ERR1", oboTenant, oboUser, opName, systemId, effectiveUserId, host,
              path, e.getMessage());
      throw new IOException(msg, e);
    }

    // Populate the FileStatInfo object
    statInfo = new FileStatInfo(absolutePathStr, sftpAttrs.getUserId(), sftpAttrs.getGroupId(),
            sftpAttrs.getSize(), sftpAttrs.getPermissions(), sftpAttrs.getAccessTime().toInstant(),
            sftpAttrs.getModifyTime().toInstant(), sftpAttrs.isDirectory(), sftpAttrs.isSymbolicLink());
    return statInfo;
  }

  /**
   * Run the linux chmod operation
   * Perms argument is validated to be an octal number between 000 and 777
   *
   * @param path - target of operation
   * @param permsStr   - perms as octal (000 through 777)
   * @param recursive  - add -R for recursive
   */
  @Override
  public NativeLinuxOpResult linuxChmod(@NotNull String path, @NotNull String permsStr, boolean recursive)
          throws TapisException, IOException, NotFoundException
  {
    String opName = "chmod";
    // Parse and validate the chmod perms argument

    try
    {
      int permsInt = Integer.parseInt(permsStr, 8);
      // Check that value is in allowed range
      if (permsInt > MAX_PERMS_INT || permsInt < 0)
      {
        String msg = LibUtils.getMsg("FILES_CLIENT_SSH_CHMOD_PERMS", oboTenant, oboUser, systemId, effectiveUserId, host,
                path, permsStr);
        throw new TapisException(msg);
      }
    }
    catch (NumberFormatException e)
    {
      String msg = LibUtils.getMsg("FILES_CLIENT_SSH_CHMOD_ERR", oboTenant, oboUser, systemId, effectiveUserId, host,
              path, permsStr, e.getMessage());
      throw new TapisException(msg, e);
    }

    // Run the command
    return runLinuxChangeOp(opName, permsStr, path, recursive);
  }

  @Override
  public NativeLinuxOpResult linuxChown(@NotNull String path, @NotNull String newOwner, boolean recursive)
          throws TapisException, IOException, NotFoundException
  {
    String opName = "chown";
    // Validate that owner is valid linux username
    if (!USER_REGEX.matcher(newOwner).matches()) {
      String msg = LibUtils.getMsg("FILES_CLIENT_SSH_LINUXOP_USRGRP", oboTenant, oboUser, systemId, effectiveUserId, host,
              path, opName, newOwner);
      throw new TapisException(msg);
    }
    // Run the command
    return runLinuxChangeOp(opName, newOwner, path, recursive);
  }

  @Override
  public NativeLinuxOpResult linuxChgrp(@NotNull String path, @NotNull String newGroup, boolean recursive)
          throws TapisException, IOException, NotFoundException
  {
    String opName = "chgrp";
    // Validate that group is valid linux group name
    if (!USER_REGEX.matcher(newGroup).matches())
    {
      String msg = LibUtils.getMsg("FILES_CLIENT_SSH_LINUXOP_USRGRP", oboTenant, oboUser, systemId, effectiveUserId, host,
              path, opName, newGroup);
      throw new TapisException(msg);
    }
    // Run the command
    return runLinuxChangeOp(opName, newGroup, path, recursive);
  }

  /* **************************************************************************** */
  /*                                Private Methods                               */
  /* **************************************************************************** */

  /**
   * Create a file
   *
   * @param path - Path to file relative to the system rootDir
   * @param fileStream Data stream to use for insert/append
   * @throws IOException Generally a network error
   */
  private void createFile(@NotNull String path, @NotNull InputStream fileStream) throws IOException
  {
    path = FilenameUtils.normalize(path);
    Path absolutePath = Paths.get(rootDir, path).normalize();
    Path relativeRemotePath = Paths.get(StringUtils.stripStart(path, "/")).normalize();
    Path parentPath = relativeRemotePath.getParent();
    try (fileStream; var sessionHolder = borrowAutoCloseableSftpClient(DEFAULT_SESSION_WAIT, true)) {
      if (parentPath != null) {
        mkdir(parentPath.toString());
      }
      OutputStream outputStream = sessionHolder.getSession().write(absolutePath.toString());
      fileStream.transferTo(outputStream);
      outputStream.flush();
      outputStream.close();
    } catch (IOException ex) {
      handleSftpException(ex, "createFile", path);
      String msg = LibUtils.getMsg("FILES_CLIENT_SSH_OP_ERR1", oboTenant, oboUser, "insertOrAppend", systemId, effectiveUserId, host, path, ex.getMessage());
      throw new IOException(msg, ex);
    }
  }

  /**
   * Recursive method to delete a file or files in a directory using the provided sftpClient
   * @param sftpClient sftp client
   * @param relPathStr path
   * @throws IOException On error
   */
  private void recursiveDelete(SSHSftpClient sftpClient, String relPathStr) throws IOException, NotSupportedException
  {
    String opName = "recursiveDelete";
    String absolutePathStr = PathUtils.getAbsolutePath(rootDir, relPathStr).toString();

    //Get list of all files at the path
    List<FileInfo> files = ls(relPathStr);
    FileInfo fileInfo = getFileInfo(relPathStr, false);

    if(fileInfo.isDir()) {
      // For each file/directory do a delete
      for (FileInfo entry : files) {
        // If it is a directory (other than . or ..) then recursively delete
        if ((!entry.getName().equals(".")) && (!entry.getName().equals("..")) && (entry.isDir())) {
          // Get a normalized path for the directory
          Path tmpPath = Paths.get(relPathStr, entry.getName()).normalize();
          recursiveDelete(sftpClient, tmpPath.toString());
        } else {
          // It is a file or link, use sftpClient to delete (this will not delete the file if the
          // type is "other" - but that's what we want
          Path tmpPath = PathUtils.getAbsolutePath(rootDir, entry.getPath());
          if (entry.isFile() || entry.isSymLink()) {
            sftpClient.remove(tmpPath.toString());
          } else {
            String msg = LibUtils.getMsg("FILES_CLIENT_SPECIAL_FILE",
                    oboTenant, oboUser, systemId, effectiveUserId, host, absolutePathStr, entry.getType(), opName);
            log.error(msg);
            throw new NotSupportedException(msg);
          }
        }
      }
    }
    // If we were working on a directory and it was not rootDir then remove the directory entry
    if (!absolutePathStr.equals(rootDir))
    {
      if(fileInfo.isDir()) {
        sftpClient.rmdir(absolutePathStr);
      } else if (fileInfo.isFile() || fileInfo.isSymLink()) {
        sftpClient.remove(absolutePathStr);
      } else {
        throw new NotSupportedException(LibUtils.getMsg("FILES_CLIENT_SPECIAL_FILE",
                oboTenant, oboUser, systemId, effectiveUserId, host, absolutePathStr, fileInfo.getType(), opName));
      }
    }
  }

  /**
   * Run one of the linux change operations: chmod, chown, chgrp
   *
   * @param opName     - Operation to execute
   * @param arg1       - argument for operation (perms as octal, new owner, new group)
   * @param path - target of operation
   * @param recursive  - add -R for recursive
   */
  private NativeLinuxOpResult runLinuxChangeOp(String opName, String arg1, String path, boolean recursive)
          throws TapisException, IOException, NotFoundException
  {
    // Make sure we have a valid first argument
    if (StringUtils.isBlank(arg1))
    {
      String msg = LibUtils.getMsg("FILES_CLIENT_SSH_LINUXOP_NOARG", oboTenant, oboUser, systemId, effectiveUserId, host,
              path, opName);
      throw new TapisException(msg);
    }
    // Path should have already been normalized and checked but for safety and security do it
    //   again here. FilenameUtils.normalize() is expected to protect against escaping via ../..
    // Get path relative to system rootDir and protect against ../..
    String relativePathStr = PathUtils.getRelativePath(path).toString();
    String absolutePathStr = PathUtils.getAbsolutePath(rootDir, relativePathStr).toString();

    // Check that path exists. This will throw a NotFoundException if path is not there
    this.ls(relativePathStr);

    // Build the command and execute it.

    try (var sessionHolder = borrowAutoCloseableExecChannel(DEFAULT_SESSION_WAIT, true)) {
      StringBuilder sb = new StringBuilder(opName);
      if (recursive) sb.append(" -R");
      sb.append(" ").append(arg1).append(" ").append(absolutePathStr);
      String cmdStr = sb.toString();
      // Execute the command
      ByteArrayOutputStream stdOut = new ByteArrayOutputStream();
      ByteArrayOutputStream stdErr = new ByteArrayOutputStream();
      int exitCode = sessionHolder.getSession().execute(cmdStr, stdOut, stdErr, false);
      if (exitCode != 0) {
        String msg = LibUtils.getMsg("FILES_CLIENT_SSH_LINUXOP_ERR", oboTenant, oboUser, systemId, effectiveUserId, host,
                path, opName, exitCode, stdOut.toString(), stdErr.toString(), cmdStr);
        log.warn(msg);
      }
      return new NativeLinuxOpResult(cmdStr, exitCode, String.valueOf(stdOut), String.valueOf(stdErr));
    }
  }

  /**
   * Get FileInfo for specified path, return null if path not found.
   *
   * @param sftpClient - Client to use for performing operation
   * @param path - Path to file or directory relative to the system rootDir
   * @return FileInfo for the path or null if path not found
   * @throws IOException on IO error
   */
  private FileInfo getFileInfo(SSHSftpClient sftpClient, String path, boolean followLinks) throws IOException, NotFoundException
  {
    FileInfo fileInfo = new FileInfo();
    // Process the relative path string and make sure it is not empty.
    // Get path relative to system rootDir and protect against ../..
    String relativePathStr = PathUtils.getRelativePath(path).toString();
    Path absolutePath = PathUtils.getAbsolutePath(rootDir, relativePathStr);
    try
    {
      // Get stat attributes and fill in FileInfo.
      Attributes attributes = followLinks ? sftpClient.stat(absolutePath.toString())
              : sftpClient.lstat(absolutePath.toString());
      fileInfo.setType(getFileInfoType(attributes));
      DirEntry entry = new DirEntry(relativePathStr, relativePathStr, attributes);
      Path entryPath = Paths.get(entry.getFilename());
      fileInfo.setName(entryPath.getFileName().toString());
      fileInfo.setLastModified(attributes.getModifyTime().toInstant());
      fileInfo.setSize(attributes.getSize());
      fileInfo.setOwner(String.valueOf(attributes.getUserId()));
      fileInfo.setGroup(String.valueOf(attributes.getGroupId()));
      fileInfo.setNativePermissions(FileStatInfo.getPermsFromInt(attributes.getPermissions()));

      //Try to determine the Mimetype
      Path tmpPath = Paths.get(entry.getFilename());
      fileInfo.setMimeType(Files.probeContentType(tmpPath));

      //Path should be relative to rootDir
      // TODO: Add more comments as to exactly why this is needed and what is going on.
      Path tmpFilePath = Paths.get(rootDir, entry.getFilename());
      if (tmpFilePath.equals(absolutePath))
      {
        String thePath = StringUtils.removeStart(entryPath.toString(), "/");
        fileInfo.setPath(thePath);
      }
      else
      {
        fileInfo.setPath(Paths.get(relativePathStr, entryPath.toString()).toString());
      }
      fileInfo.setUrl(PathUtils.getTapisUrlFromPath(fileInfo.getPath(), systemId));
    }
    catch (IOException e)
    {
      // If due to NotFound then return null, else re-throw the IOException
      if (e.getMessage().toLowerCase().contains(NO_SUCH_FILE)) return null;
      else throw e;
    }
    return fileInfo;
  }

  public List<AclEntry> runLinuxGetfacl(String path) throws IOException, TapisException {
    String opName = "getfacl";

    // Check that path exists. This will throw a NotFoundException if path is not there
    String relativePathStr = PathUtils.getRelativePath(path).toString();
    String absolutePathStr = PathUtils.getAbsolutePath(rootDir, relativePathStr).toString();
    this.ls(relativePathStr);

    // Build the command and execute it.
    try (var sessionHolder = borrowAutoCloseableExecChannel(DEFAULT_SESSION_WAIT, true)) {
      StringBuilder sb = new StringBuilder(opName);
      sb.append(" -cpE ").append(safelySingleQuoteString(absolutePathStr));
      String cmdStr = sb.toString();

      // Execute the command
      ByteArrayOutputStream stdOut = new ByteArrayOutputStream();
      ByteArrayOutputStream stdErr = new ByteArrayOutputStream();
      int exitCode = sessionHolder.getSession().execute(cmdStr, stdOut, stdErr, false);
      if (exitCode != 0) {
        String msg = LibUtils.getMsg("FILES_CLIENT_SSH_LINUXOP_ERR", oboTenant, oboUser, systemId, effectiveUserId, host,
                path, opName, exitCode, stdOut.toString(), stdErr.toString(), cmdStr);
        log.warn(msg);
      }
      if (exitCode != 0) {
        StringBuilder msg = new StringBuilder("Native Linux operation getfacl returned a non-zero exit code.");
        msg.append(System.lineSeparator());
        msg.append(stdErr);
        msg.append(System.lineSeparator());
        throw new TapisException(msg.toString());
      }

      return AclEntry.parseAclEntries(String.valueOf(stdOut));
    }
  }

  @Override
  public NativeLinuxOpResult runLinuxSetfacl(String path, FileUtilsService.NativeLinuxFaclOperation operation,
                                             FileUtilsService.NativeLinuxFaclRecursion recursion,
                                             String aclString) throws IOException, TapisException {
    String opName = "setfacl";

    // Path should have already been normalized and checked but for safety and security do it
    //   again here. FilenameUtils.normalize() is expected to protect against escaping via ../..
    // Get path relative to system rootDir and protect against ../..
    String relativePathStr = PathUtils.getRelativePath(path).toString();
    String absolutePathStr = PathUtils.getAbsolutePath(rootDir, relativePathStr).toString();

    // Check that path exists. This will throw a NotFoundException if path is not there
    this.ls(relativePathStr);

    // Build the command and execute it.

    StringBuilder sb = new StringBuilder(opName);
    sb.append(" ");

    switch (recursion) {
      // recurse - don't follow symlinks
      case PHYSICAL -> sb.append("-RP ");
      // recurse - follow symlinks
      case LOGICAL -> sb.append("-RL ");
    }

    switch (operation) {
      // add ACLs
      case ADD -> sb.append("-m ").append(safelySingleQuoteString(aclString)).append(" ");

      // remove ACLs
      case REMOVE -> sb.append("-x ").append(safelySingleQuoteString(aclString)).append(" ");

      // remove all ACLs
      case REMOVE_ALL -> sb.append("-b ");

      // remove all "default:" ACLs
      case REMOVE_DEFAULT -> sb.append("-k ");
    }

    sb.append(safelySingleQuoteString(absolutePathStr));

    String cmdStr = sb.toString();

    // Execute the command
    ByteArrayOutputStream stdOut = new ByteArrayOutputStream();
    ByteArrayOutputStream stdErr = new ByteArrayOutputStream();
    int exitCode = 0;
    try (var sessionHolder = borrowAutoCloseableExecChannel(DEFAULT_SESSION_WAIT, true)) {
      exitCode = sessionHolder.getSession().execute(cmdStr, stdOut, stdErr, false);
      if (exitCode != 0) {
        String msg = LibUtils.getMsg("FILES_CLIENT_SSH_LINUXOP_ERR", oboTenant, oboUser, systemId, effectiveUserId, host,
                path, opName, exitCode, stdOut.toString(), stdErr.toString(), cmdStr);
        log.warn(msg);
      }
    } catch (IOException | TapisException ex) {
      // log some information about the failure, then rethrow the exception
      String msg = LibUtils.getMsg("FILES_CLIENT_SSH_LINUXOP_ERR", oboTenant, oboUser, systemId, effectiveUserId, host,
              path, opName, exitCode, stdOut.toString(), stdErr.toString(), cmdStr);
      log.error(msg);
    }

    return new NativeLinuxOpResult(cmdStr, exitCode, String.valueOf(stdOut), String.valueOf(stdErr));
  }

  // This method will single quote a string and convert all embedded single quotes
  // into '\'' - so file'name would be converted to 'file'\''name'.  This is to prevent
  // unix command injection (something like embedding ;rm -rf / in a command that we will
  // execute in the bash shell.
  private String safelySingleQuoteString(String unquotedString) {
    StringBuilder sb = new StringBuilder();
    sb.append("'");
    sb.append(unquotedString.replace("'", "'\\''"));
    sb.append("'");
    return sb.toString();
  }

  private FileInfo.FileType getFileInfoType(Attributes attributes) {
    if(attributes.isDirectory()) {
      return FileInfo.FileType.DIR;
    } else if (attributes.isRegularFile()) {
      return FileInfo.FileType.FILE;
    } else if (attributes.isSymbolicLink()) {
      return FileInfo.FileType.SYMBOLIC_LINK;
    } else if (attributes.isOther()) {
      return FileInfo.FileType.OTHER;
    } else {
      return FileInfo.FileType.UNKNOWN;
    }
  }

  // retryOnFail - if this is set to true and the call to borrow a session fails with a tapis recoverable exception, the
  //               code will invalidate the system cache for that system, and try again (re-obtaining the system credentials).
  //               If the second try fails, the method will throw an IOException.  This behavior should help with caching
  //               issues around credential changes.
  private SshSessionPool.PooledSshSession<SSHExecChannel> borrowAutoCloseableExecChannel(Duration wait, boolean retryOnFail) throws IOException {
    try {
      TapisSystem cacheSystem = getSystemForConnection();
      return SshSessionPool.getInstance().borrowExecChannel(cacheSystem.getTenant(), cacheSystem.getHost(), cacheSystem.getPort(),
              cacheSystem.getEffectiveUserId(), cacheSystem.getDefaultAuthnMethod(), cacheSystem.getAuthnCredential(), wait);
    } catch (TapisRecoverableException ex) {
      systemsCache.invalidateEntry(system.getTenant(), system.getId(), system.getEffectiveUserId(), impersonationId, sharedCtxGrantor);
      if(retryOnFail) {
        return borrowAutoCloseableExecChannel(wait, false);
      } else {
        String msg = LibUtils.getMsg("FILES_CLIENT_SSH_SESSION_POOL_ERROR", system.getTenant(),
                system.getHost(), system.getPort(), system.getEffectiveUserId(), system.getDefaultAuthnMethod(), wait, ex.getMessage());
        if(ex instanceof TapisSSHAuthException) {
          throw new NotAuthorizedException(msg, ex);
        }
        throw new IOException(msg);
      }
    } catch (TapisException ex) {
      String msg = LibUtils.getMsg("FILES_CLIENT_SSH_SESSION_POOL_ERROR", system.getTenant(),
              system.getHost(), system.getPort(), system.getEffectiveUserId(), system.getDefaultAuthnMethod(), wait, ex.getMessage());
      if(ex instanceof TapisSSHAuthException) {
        throw new NotAuthorizedException(msg, ex);
      }
      throw new IOException(ex.getMessage(), ex);
    }
  }


  // retryOnFail - if this is set to true and the call to borrow a session fails with a tapis recoverable exception, the
  //               code will invalidate the system cache for that system, and try again (re-obtaining the system credentials).
  //               If the second try fails, the method will throw an IOException.  This behavior should help with caching
  //               issues around credential changes.
  private SshSessionPool.PooledSshSession<SSHSftpClient> borrowAutoCloseableSftpClient(Duration wait, boolean retryOnFail) throws IOException {
    try {
      TapisSystem cacheSystem = getSystemForConnection();
      return SshSessionPool.getInstance().borrowSftpClient(cacheSystem.getTenant(), cacheSystem.getHost(), cacheSystem.getPort(),
              cacheSystem.getEffectiveUserId(), cacheSystem.getDefaultAuthnMethod(), cacheSystem.getAuthnCredential(), wait);
    } catch (TapisRecoverableException ex) {
      systemsCache.invalidateEntry(system.getTenant(), system.getId(), system.getEffectiveUserId(), impersonationId, sharedCtxGrantor);
      if(retryOnFail) {
        return borrowAutoCloseableSftpClient(wait, false);
      } else {
        String msg = LibUtils.getMsg("FILES_CLIENT_SSH_SESSION_POOL_ERROR", system.getTenant(),
                system.getHost(), system.getPort(), system.getEffectiveUserId(), system.getDefaultAuthnMethod(), wait, ex.getMessage());
        if(ex instanceof TapisSSHAuthException) {
          throw new NotAuthorizedException(msg, ex);
        }
        throw new IOException(msg);
      }
    } catch (TapisException ex) {
      String msg = LibUtils.getMsg("FILES_CLIENT_SSH_SESSION_POOL_ERROR", system.getTenant(),
              system.getHost(), system.getPort(), system.getEffectiveUserId(), system.getDefaultAuthnMethod(), wait, ex.getMessage());
      if(ex instanceof TapisSSHAuthException) {
        throw new NotAuthorizedException(msg, ex);
      }
      throw new IOException(ex.getMessage(), ex);
    }
  }

  private void handleSftpException(Exception ex, String operation, String path) throws IOException {
    // sftp exceptions can be wrapped in runtime exceptions
    if((ex instanceof RuntimeException) && (ex.getCause() instanceof SftpException)) {
      ex = (Exception) ex.getCause();
    }

    if (ex instanceof SftpException) {
      SftpException sftpException = (SftpException) ex;
      if (sftpException.getStatus() == SftpConstants.SSH_FX_PERMISSION_DENIED) {
        String msg = LibUtils.getMsg("FILES_CLIENT_SSH_OP_ERR1", oboTenant, oboUser, operation, systemId, effectiveUserId, host, path, ex.getMessage());
        throw new ForbiddenException(msg, ex);
      } else if (sftpException.getStatus() == SftpConstants.SSH_FX_NO_SUCH_FILE) {
        String msg = LibUtils.getMsg("FILES_CLIENT_SSH_NOT_FOUND", oboTenant, oboUser, systemId, effectiveUserId, host, rootDir, path);
        throw new NotFoundException(msg, ex);
      } else {
        String msg = LibUtils.getMsg("FILES_CLIENT_SSH_OP_ERR1", oboTenant, oboUser, operation, systemId, effectiveUserId, host, path, ex.getMessage());
        throw new IOException(msg, ex);
      }
    } else if ((ex.getMessage() != null) && (ex.getMessage().contains("SSH_POOL_MISSING_CREDENTIALS"))) {
      throw new NotAuthorizedException(ex.getMessage(), ex);
    }
  }

  private TapisSystem getSystemForConnection() {
    // this method is important.  We need the ability to refresh our copy of the tapis system.  The method that borrows
    // the ssh sessions will invalidate the cache in the event of a failure, in case the creds have changed, so we need to
    // ask for it again so we get the refreshed creds
    try {
      TapisSystem cachedSystem = systemsCache.getSystem(oboTenant, systemId, oboUser, impersonationId, sharedCtxGrantor);
      return cachedSystem;
    } catch (ServiceException ex) {
      // ignore the failure here.  There's really nothing we can do.  We have a copy of the system already and it may
      // or may not be the latest, but it's the best we can do.  I think this is better than an exception since there
      // is at least some chance it will succeed.  Besides, if it does fail we will there the exception there anyway.
    }
    return system;
  }

}
