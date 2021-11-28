package edu.utexas.tacc.tapis.files.lib.clients;

import edu.utexas.tacc.tapis.files.lib.caches.SSHConnectionHolder;
import edu.utexas.tacc.tapis.files.lib.models.FileInfo;
import edu.utexas.tacc.tapis.files.lib.models.FileStatInfo;
import edu.utexas.tacc.tapis.files.lib.models.NativeLinuxOpResult;
import edu.utexas.tacc.tapis.files.lib.utils.PathUtils;
import edu.utexas.tacc.tapis.files.lib.utils.Utils;
import edu.utexas.tacc.tapis.shared.exceptions.TapisException;
import edu.utexas.tacc.tapis.shared.ssh.apache.SSHExecChannel;
import edu.utexas.tacc.tapis.shared.ssh.apache.SSHSftpClient;
import edu.utexas.tacc.tapis.systems.client.gen.model.TapisSystem;
import org.apache.commons.exec.CommandLine;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.sshd.sftp.client.SftpClient.Attributes;
import org.apache.sshd.sftp.client.SftpClient.DirEntry;
import org.apache.sshd.sftp.common.SftpException;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.NotFoundException;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

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

    private final Logger log = LoggerFactory.getLogger(SSHDataClient.class);

    private final String oboTenant;
    private final String oboUser;

    private final String host;
    private final String username;
    private final String rootDir;
    private final String systemId;
    private final TapisSystem tapisSystem;
    private final SSHConnectionHolder connectionHolder;

    public String getOboTenant() { return oboTenant; }
    public String getOboUser() { return oboUser; }
    public String getSystemId() { return systemId; }
    public String getUsername() { return username; }
    public String getHost() { return host; }

    // Username must start with letter/underscore, contain alphanumeric or _ or -, have at most 32 characters
    //   and may end with $
    private static final Pattern USER_REGEX = Pattern.compile("^[a-z_]([a-z0-9_-]{0,31}|[a-z0-9_-]{0,30}\\$)$");

    public SSHDataClient(@NotNull String oboTenant1, @NotNull String oboUser1, @NotNull TapisSystem sys,
                         @NotNull SSHConnectionHolder holder)
    {
        oboTenant = oboTenant1;
        oboUser = oboUser1;
        rootDir = PathUtils.getAbsolutePath(sys.getRootDir(), "/").toString();
        host = sys.getHost();
        username = sys.getEffectiveUserId();
        systemId = sys.getId();
        tapisSystem = sys;
        connectionHolder = holder;
    }

    public List<FileInfo> ls(@NotNull String path) throws IOException, NotFoundException {
        return ls(path, MAX_LISTING_SIZE, 0);
    }

    /**
     * Return file listing on path using sftpClient
     *
     * @param path - Path to file or directory relative to the system rootDir
     * @return list of FileInfo objects
     * @throws IOException       Generally a network error
     * @throws NotFoundException No file at target
     */
    @Override
    public List<FileInfo> ls(@NotNull String path, long limit, long offset) throws IOException, NotFoundException
    {
      long count = Math.min(limit, MAX_LISTING_SIZE);
      long startIdx = Math.max(offset, 0);
      List<FileInfo> filesList = new ArrayList<>();
      List<DirEntry> dirEntries = new ArrayList<>();
      String relPathStr = PathUtils.getRelativePath(path).toString();
      Path absolutePath = PathUtils.getAbsolutePath(rootDir, relPathStr);
        SSHSftpClient sftpClient = connectionHolder.getSftpClient();
        try {
            Attributes attributes = sftpClient.stat(absolutePath.toString());
            if (attributes.isDirectory()) {
                Iterable<DirEntry> tmp = sftpClient.readDir(absolutePath.toString());
                tmp.forEach(dirEntries::add);
            } else {
                DirEntry entry = new DirEntry(relPathStr, relPathStr, attributes);
                dirEntries.add(entry);
            }
        } catch (IOException e) {
            if (e.getMessage().toLowerCase().contains(NO_SUCH_FILE)) {
                String msg = Utils.getMsg("FILES_CLIENT_SSH_NOT_FOUND", oboTenant, oboUser, systemId, username, host, rootDir, relPathStr);
                throw new NotFoundException(msg);
            } else {
                String msg = Utils.getMsg("FILES_CLIENT_SSH_OP_ERR1", oboTenant, oboUser, "ls", systemId, username, host, relPathStr, e.getMessage());
                throw new IOException(msg, e);
            }
        } finally {
            sftpClient.close();
            connectionHolder.returnSftpClient(sftpClient);
        }

        // For each entry in the fileList received, get the fileInfo object
        for (DirEntry entry: dirEntries) {
            // Get the file attributes
            Attributes attrs = entry.getAttributes();
            FileInfo fileInfo = new FileInfo();
            // Ignore filename . and ..
            if (entry.getFilename().equals(".") || entry.getFilename().equals("..")) {
                continue;
            }
            Path entryPath = Paths.get(entry.getFilename());
            fileInfo.setName(entryPath.getFileName().toString());
            fileInfo.setLastModified(attrs.getModifyTime().toInstant());
            fileInfo.setSize(attrs.getSize());

            //Try to determine the Mimetype
            Path tmpPath = Paths.get(entry.getFilename());
            fileInfo.setMimeType(Files.probeContentType(tmpPath));
            if (attrs.isDirectory()) {
                fileInfo.setType(FileInfo.FILETYPE_DIR);
            } else {
                fileInfo.setType(FileInfo.FILETYPE_FILE);
            }
            fileInfo.setOwner(String.valueOf(attrs.getUserId()));
            fileInfo.setGroup(String.valueOf(attrs.getGroupId()));
            fileInfo.setNativePermissions(FileStatInfo.getPermsFromInt(attrs.getPermissions()));
            //Path should be relative to rootDir
            // TODO: Add more comments as to exactly why this is needed and what is going on.
            Path tmpFilePath = Paths.get(rootDir, entry.getFilename());
            if (tmpFilePath.equals(absolutePath)) {
                String thePath = StringUtils.removeStart(entryPath.toString(), "/");
                fileInfo.setPath(thePath);
            } else {
                fileInfo.setPath(Paths.get(relPathStr, entryPath.toString()).toString());
            }
            filesList.add(fileInfo);
        }
        filesList.sort(Comparator.comparing(FileInfo::getName));
        return filesList.stream().skip(startIdx).limit(count).collect(Collectors.toList());
    }

    /**
     * Create a directory using sftpClient
     * Directories in path will be created as necessary.
     *
     * @param path Normalized path relative to system rootDir
     * @throws IOException Generally a network error
     */
    @Override
    public void mkdir(@NotNull String path) throws IOException
    {
        path = FilenameUtils.normalize(path);
        Path remote = Paths.get(rootDir, path);
        Path rootDirPath = Paths.get(rootDir);
        Path relativePath = rootDirPath.relativize(remote);
        SSHSftpClient sftpClient = connectionHolder.getSftpClient();
        try {
            Path tmpPath = Paths.get(rootDir);
            // Walk the path parts creating directories
            for (Path part: relativePath) {
                tmpPath = tmpPath.resolve(part);
                try {
                  // TODO do a stat to see if dir exists rather than catch/ignore exception
                  // TODO: Check to see if path exists and is a file. That should return an error
                    sftpClient.mkdir(tmpPath.toString());
                } catch (SftpException ignored) {}
            }
        }
        finally {
            sftpClient.close();
            connectionHolder.returnSftpClient(sftpClient);
        }
    }

    @Override
    public void upload(@NotNull String path, @NotNull InputStream fileStream) throws IOException
    {
      insertOrAppend(path, fileStream, false);
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
    public void move(@NotNull String srcPath, @NotNull String dstPath) throws IOException, NotFoundException
    {
      String relOldPathStr = PathUtils.getRelativePath(srcPath).toString();
      String relNewPathStr = PathUtils.getRelativePath(dstPath).toString();
      Path absoluteOldPath = PathUtils.getAbsolutePath(rootDir, relOldPathStr);
      Path absoluteNewPath = PathUtils.getAbsolutePath(rootDir, relNewPathStr);
      SSHSftpClient sftpClient = connectionHolder.getSftpClient();
      try
      {
        // If newPath is an existing directory then append the oldPath file/dir name to the newPath
        //  so the oldPath is moved into the target directory.
        FileInfo fileInfo = getFileInfo(sftpClient, relNewPathStr);
        if (fileInfo != null && fileInfo.isDir()) {
          absoluteNewPath = Paths.get(absoluteNewPath.toString(), absoluteOldPath.getFileName().toString());
        }
        sftpClient.rename(absoluteOldPath.toString(), absoluteNewPath.toString());
      }
      catch (IOException e)
      {
        if (e.getMessage().toLowerCase().contains(NO_SUCH_FILE)) {
          String msg = Utils.getMsg("FILES_CLIENT_SSH_NOT_FOUND", oboTenant, oboUser, systemId, username, host, rootDir, srcPath);
          throw new NotFoundException(msg);
        } else {
          String msg = Utils.getMsg("FILES_CLIENT_SSH_OP_ERR2", oboTenant, oboUser, "move", systemId, username, host, srcPath, dstPath, e.getMessage());
          throw new IOException(msg, e);
        }
      }
      finally
      {
        sftpClient.close();
        connectionHolder.returnSftpClient(sftpClient);
      }
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
        String relOldPathStr = PathUtils.getRelativePath(srcPath).toString();
        String relNewPathStr = PathUtils.getRelativePath(dstPath).toString();
        Path absoluteOldPath = PathUtils.getAbsolutePath(rootDir, relOldPathStr);
        Path absoluteNewPath = PathUtils.getAbsolutePath(rootDir, relNewPathStr);
        Path targetParentPath = absoluteNewPath.getParent();

        //This will throw a NotFoundException if source is not there
        ls(relOldPathStr);

        // Construct and run linux commands to create the target dir and do the copy
        SSHExecChannel channel = connectionHolder.getExecChannel();
        try  {
            // Set up arguments
            Map<String, String> args = new HashMap<>();
            args.put("targetParentPath", targetParentPath.toString());
            args.put("source", absoluteOldPath.toString());
            args.put("target", absoluteNewPath.toString());
            // Command to make the directory including any intermediate directories
            CommandLine cmd = new CommandLine("mkdir").addArgument("-p").addArgument("${targetParentPath}").addArgument(";");
            // Command to do the copy
            cmd.addArgument("cp").addArgument("${source}").addArgument("${target}");
            // Fill in arguments and execute
            cmd.setSubstitutionMap(args);
            String toExecute = String.join(" ", cmd.toStrings());
            channel.execute(toExecute);
        } catch (TapisException e) {
            if (e.getMessage().toLowerCase().contains(NO_SUCH_FILE)) {
                String msg = Utils.getMsg("FILES_CLIENT_SSH_NOT_FOUND", oboTenant, oboUser, systemId, username, host, rootDir, relOldPathStr);
                throw new NotFoundException(msg);
            } else {
                String msg = Utils.getMsg("FILES_CLIENT_SSH_OP_ERR2", oboTenant, oboUser, "copy", systemId, username, host, relOldPathStr, relNewPathStr, e.getMessage());
                throw new IOException(msg, e);
            }
        } finally {
            connectionHolder.returnExecChannel(channel);
        }
    }

    /**
     * Delete a file or directory using sftpClient
     *
     * @param path - Path to file or directory relative to the system rootDir
     * @throws IOException Generally a network error
     */
    @Override
    public void delete(@NotNull String path) throws IOException
    {
      String relativePathStr = PathUtils.getRelativePath(path).toString();
      SSHSftpClient sftpClient = connectionHolder.getSftpClient();
      try  {
        recursiveDelete(sftpClient, relativePathStr);
      } catch (IOException e) {
        if (e.getMessage().toLowerCase().contains(NO_SUCH_FILE)) {
          String msg = Utils.getMsg("FILES_CLIENT_SSH_NOT_FOUND", oboTenant, oboUser, systemId, username, host, rootDir, relativePathStr);
          throw new NotFoundException(msg);
        } else {
          String msg = Utils.getMsg("FILES_CLIENT_SSH_OP_ERR1", oboTenant, oboUser, "delete", systemId, username, host, relativePathStr, e.getMessage());
          throw new IOException(msg, e);
        }
      } finally {
        sftpClient.close();
        connectionHolder.returnSftpClient(sftpClient);
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
      SSHSftpClient sftpClient = connectionHolder.getSftpClient();
      try {
        InputStream inputStream = sftpClient.read(absPath.toString());
        //TapisSSHInputStream closes the sftp connection after reading completes
        return new TapisSSHInputStream(inputStream, connectionHolder, sftpClient);
      } catch (IOException e) {
        connectionHolder.returnSftpClient(sftpClient);
        if (e.getMessage().toLowerCase().contains(NO_SUCH_FILE)) {
          String msg = Utils.getMsg("FILES_CLIENT_SSH_NOT_FOUND", oboTenant, oboUser, systemId, username, host, rootDir, path);
          throw new NotFoundException(msg);
        } else {
          String msg = Utils.getMsg("FILES_CLIENT_SSH_OP_ERR1", oboTenant, oboUser, "getStream", systemId, username, host, path, e.getMessage());
          log.error(msg, e);
          throw new IOException(msg, e);
        }
      }
    }

    @Override
    public InputStream getBytesByRange(@NotNull String path, long startByte, long count) throws IOException {
        Path absPath = PathUtils.getAbsolutePath(rootDir, path);
        SSHExecChannel channel = connectionHolder.getExecChannel();
        try {
            //TODO: This should use Piped streams
            String command = String.format("dd if=%s ibs=1 skip=%s count=%s", absPath.toString(), startByte, count);
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            ByteArrayOutputStream outErr = new ByteArrayOutputStream();
            channel.execute(command, out, outErr);
            connectionHolder.returnExecChannel(channel);
            return new ByteArrayInputStream(out.toByteArray());

        } catch (TapisException e) {
            //IMPORTANT: Have to return the channel here until we implement PipedInput/PipedOutput streams.
            connectionHolder.returnExecChannel(channel);
            if (e.getMessage().toLowerCase().contains(NO_SUCH_FILE)) {
                String msg = Utils.getMsg("FILES_CLIENT_SSH_NOT_FOUND", oboTenant, oboUser, systemId, username, host, rootDir, path);
                throw new NotFoundException(msg);
            } else {
                String msg = Utils.getMsg("FILES_CLIENT_SSH_OP_ERR1", oboTenant, oboUser, "getBytesByRange", systemId, username, host, path, e.getMessage());
                log.error(msg, e);
                throw new IOException(msg, e);
            }
        }
    }

    @Override
    public void putBytesByRange(String path, InputStream byteStream, long startByte, long endByte) throws IOException
    {
    }

    @Override
    public void append(@NotNull String path, @NotNull InputStream byteStream) throws IOException
    {
        insertOrAppend(path, byteStream, true);
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
        String relativePathStr = PathUtils.getRelativePath(path).toString();
        String absolutePathStr = PathUtils.getAbsolutePath(rootDir, relativePathStr).toString();
        Attributes sftpAttrs;
        SSHSftpClient sftpClient = connectionHolder.getSftpClient();
        try {
            // If path is a symbolic link then stat gives info for the link target, lstat gives info for the link
            sftpAttrs = followLinks ? sftpClient.stat(absolutePathStr) : sftpClient.lstat(absolutePathStr);
        } catch (IOException e) {
            if (e.getMessage().toLowerCase().contains(NO_SUCH_FILE)) {
                String msg = Utils.getMsg("FILES_CLIENT_SSH_NOT_FOUND", oboTenant, oboUser, systemId, username, host, rootDir, path);
                throw new NotFoundException(msg);
            } else {
                String msg = Utils.getMsg("FILES_CLIENT_SSH_OP_ERR1", oboTenant, oboUser, opName, systemId, username, host,
                    path, e.getMessage());
                throw new IOException(msg, e);
            }
        } finally {
            connectionHolder.returnSftpClient(sftpClient);
            sftpClient.close();
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
        try {
            int permsInt = Integer.parseInt(permsStr, 8);
            // Check that value is in allowed range
            if (permsInt > MAX_PERMS_INT || permsInt < 0) {
                String msg = Utils.getMsg("FILES_CLIENT_SSH_CHMOD_PERMS", oboTenant, oboUser, systemId, username, host,
                    path, permsStr);
                throw new TapisException(msg);
            }
        } catch (NumberFormatException e) {
            String msg = Utils.getMsg("FILES_CLIENT_SSH_CHMOD_ERR", oboTenant, oboUser, systemId, username, host,
                path, permsStr, e.getMessage());
            throw new TapisException(msg, e);
        }
        // Run the command
        return runLinuxChangeOp(opName, permsStr, path, recursive);
    }

    @Override
    public NativeLinuxOpResult linuxChown(@NotNull String path, @NotNull String newOwner, boolean recursive)
        throws TapisException, IOException, NotFoundException {
        String opName = "chown";
        // Validate that owner is valid linux user name
        if (!USER_REGEX.matcher(newOwner).matches()) {
            String msg = Utils.getMsg("FILES_CLIENT_SSH_LINUXOP_USRGRP", oboTenant, oboUser, systemId, username, host,
                path, opName, newOwner);
            throw new TapisException(msg);
        }
        // Run the command
        return runLinuxChangeOp(opName, newOwner, path, recursive);
    }

    @Override
    public NativeLinuxOpResult linuxChgrp(@NotNull String path, @NotNull String newGroup, boolean recursive)
        throws TapisException, IOException, NotFoundException {
        String opName = "chgrp";
        // Validate that group is valid linux group name
        if (!USER_REGEX.matcher(newGroup).matches()) {
            String msg = Utils.getMsg("FILES_CLIENT_SSH_LINUXOP_USRGRP", oboTenant, oboUser, systemId, username, host,
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
   * Create a file or append to a file
   *
   * @param path - Path to file relative to the system rootDir
   * @param fileStream Data stream to use for insert/append
   * @param append Flag indicating it is to be an append or an insert
   * @throws IOException Generally a network error
   */
  private void insertOrAppend(@NotNull String path, @NotNull InputStream fileStream, @NotNull Boolean append)
          throws IOException
  {
    path = FilenameUtils.normalize(path);
    Path absolutePath = Paths.get(rootDir, path).normalize();
    Path relativeRemotePath = Paths.get(StringUtils.stripStart(path, "/")).normalize();
    Path parentPath = relativeRemotePath.getParent();
    SSHSftpClient sftpClient = connectionHolder.getSftpClient();
    try (fileStream) {
      if (parentPath != null) mkdir(parentPath.toString());
      OutputStream outputStream = sftpClient.write(absolutePath.toString());
      fileStream.transferTo(outputStream);
      outputStream.close();
    } catch (IOException ex) {
      String msg = Utils.getMsg("FILES_CLIENT_SSH_OP_ERR1", oboTenant, oboUser, "insertOrAppend", systemId, username, host, path, ex.getMessage());
      throw new IOException(msg, ex);
    } finally {
      sftpClient.close();
      connectionHolder.returnSftpClient(sftpClient);
    }
  }

  /**
   * Recursive method to delete a file or files in a directory using the provided sftpClient
   * @param sftpClient sftp client
   * @param relPathStr path
   * @throws IOException On error
   */
  private void recursiveDelete(SSHSftpClient sftpClient, String relPathStr) throws IOException
  {
    String absolutePathStr = PathUtils.getAbsolutePath(rootDir, relPathStr).toString();
    //Get list of all files at the path
    List<FileInfo> files = ls(relPathStr);
    FileStatInfo attributes = getStatInfo(relPathStr, false);

    // For each file/directory do a delete
    for (FileInfo entry : files)
    {
      // If it is a directory (other than . or ..) then recursively delete
      if ((!entry.getName().equals(".")) && (!entry.getName().equals("..")) && (entry.isDir()))
      {
        // Get a normalized path for the directory
        Path tmpPath = Paths.get(relPathStr, entry.getName()).normalize();
        recursiveDelete(sftpClient, tmpPath.toString());
      }
      else
      {
        //It is a file, use sftpClient to delete
        Path tmpPath = PathUtils.getAbsolutePath(rootDir, entry.getPath());
        sftpClient.remove(tmpPath.toString());
      }
    }
    // If we were working on a directory and it was not rootDir then remove the directory entry
    if (!absolutePathStr.equals(rootDir) && attributes.isDir())
    {
      sftpClient.rmdir(absolutePathStr);
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
        if (StringUtils.isBlank(arg1)) {
            String msg = Utils.getMsg("FILES_CLIENT_SSH_LINUXOP_NOARG", oboTenant, oboUser, systemId, username, host,
                path, opName);
            throw new TapisException(msg);
        }
        // Path should have already been normalized and checked but for safety and security do it
        //   again here. FilenameUtils.normalize() is expected to protect against escaping via ../..
        String relativePathStr = PathUtils.getRelativePath(path).toString();
        String absolutePathStr = PathUtils.getAbsolutePath(rootDir, relativePathStr).toString();

        // Check that path exists. This will throw a NotFoundException if path is not there
        this.ls(relativePathStr);

        // Build the command and execute it.
        var cmdRunner = connectionHolder.getExecChannel();

        StringBuilder sb = new StringBuilder(opName);
        if (recursive) sb.append(" -R");
        sb.append(" ").append(arg1).append(" ").append(absolutePathStr);
        String cmdStr = sb.toString();
        // Execute the command
        ByteArrayOutputStream stdOut = new ByteArrayOutputStream();
        ByteArrayOutputStream stdErr = new ByteArrayOutputStream();
        int exitCode = cmdRunner.execute(cmdStr, stdOut, stdErr);
        if (exitCode != 0) {
            String msg = Utils.getMsg("FILES_CLIENT_SSH_LINUXOP_ERR", oboTenant, oboUser, systemId, username, host,
                path, opName, exitCode, stdOut.toString(), stdErr.toString());
            log.warn(msg);
        }
        connectionHolder.returnExecChannel(cmdRunner);
        return new NativeLinuxOpResult(cmdStr, exitCode, String.valueOf(stdOut), String.valueOf(stdErr));
    }

  /**
   * Get FileInfo for specified path, return null if path not found.
   *
   * @param sftpClient - Client to use for performing operation
   * @param path - Path to file or directory relative to the system rootDir
   * @return FileInfo for the path or null if path not found
   * @throws IOException on IO error
   */
  private FileInfo getFileInfo(SSHSftpClient sftpClient, String path) throws IOException, NotFoundException
  {
    FileInfo fileInfo = new FileInfo();
    // Process the relative path string and make sure it is not empty.
    String relativePathStr = PathUtils.getRelativePath(path).toString();
    Path absolutePath = PathUtils.getAbsolutePath(rootDir, relativePathStr);
    try
    {
      // Get stat attributes and fill in FileInfo.
      Attributes attributes = sftpClient.stat(absolutePath.toString());
      if (attributes.isDirectory()) fileInfo.setType(FileInfo.FILETYPE_DIR); else fileInfo.setType(FileInfo.FILETYPE_FILE);
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
      if (tmpFilePath.equals(absolutePath)) {
        String thePath = StringUtils.removeStart(entryPath.toString(), "/");
        fileInfo.setPath(thePath);
      } else {
        fileInfo.setPath(Paths.get(relativePathStr, entryPath.toString()).toString());
      }
    }
    catch (IOException e)
    {
      // If due to NotFound then return null, else re-throw the IOException
      if (e.getMessage().toLowerCase().contains(NO_SUCH_FILE)) return null;
      else throw e;
    }
    return fileInfo;
  }
}
