package edu.utexas.tacc.tapis.files.lib.clients;

import edu.utexas.tacc.tapis.files.lib.caches.SSHConnectionHolder;
import edu.utexas.tacc.tapis.files.lib.models.FileInfo;
import edu.utexas.tacc.tapis.files.lib.models.FileStatInfo;
import edu.utexas.tacc.tapis.files.lib.models.NativeLinuxOpResult;
import edu.utexas.tacc.tapis.files.lib.utils.Constants;
import edu.utexas.tacc.tapis.files.lib.utils.Utils;
import edu.utexas.tacc.tapis.shared.exceptions.TapisException;
import edu.utexas.tacc.tapis.shared.ssh.apache.SSHConnection;
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
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
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

/**
 * This class is the entry point to sile operations over SSH with Tapis.
 * All path parameters as inputs to methods are assumed to be relative to the rootDir
 * of the system unless otherwise specified.
 */
public class SSHDataClient implements ISSHDataClient {

    private final Logger log = LoggerFactory.getLogger(SSHDataClient.class);

    public String getOboTenant() {
        return oboTenant;
    }

    public String getOboUser() {
        return oboUser;
    }

    public String getSystemId() {
        return systemId;
    }

    public String getUsername() {
        return username;
    }

    public String getHost() {
        return host;
    }

    private final String oboTenant;
    private final String oboUser;

    private final String host;
    private final String username;
    private final String rootDir;
    private final String systemId;
    private final TapisSystem tapisSystem;
    private final SSHConnectionHolder connectionHolder;

    private static final int MAX_LISTING_SIZE = Constants.MAX_LISTING_SIZE;
    private static final int MAX_PERMS_INT = Integer.parseInt("777", 8);

    // Username must start with letter/underscore, contain alphanumeric or _ or -, have at most 32 characters
    //   and may end with $
    private static final Pattern USER_REGEX = Pattern.compile("^[a-z_]([a-z0-9_-]{0,31}|[a-z0-9_-]{0,30}\\$)$");

    public SSHDataClient(@NotNull String oboTenant1, @NotNull String oboUser1, @NotNull TapisSystem sys, SSHConnectionHolder holder) {
        oboTenant = oboTenant1;
        oboUser = oboUser1;
        String rdir = sys.getRootDir();
        rdir = StringUtils.isBlank(rdir) ? "/" : rdir;
        rootDir = Paths.get(rdir).normalize().toString();
        host = sys.getHost();
        username = sys.getEffectiveUserId();
        systemId = sys.getId();
        tapisSystem = sys;
        connectionHolder = holder;
    }

    private void listDirectoryRec(String basePath, List<FileInfo> listing) throws IOException, NotFoundException {
        List<FileInfo> currentListing = this.ls(basePath);
        listing.addAll(currentListing);
        for (FileInfo fileInfo : currentListing) {
            if (fileInfo.isDir()) {
                listDirectoryRec(fileInfo.getPath(), listing);
            }
        }
    }


    public List<FileInfo> ls(@NotNull String remotePath) throws IOException, NotFoundException {
        return this.ls(remotePath, MAX_LISTING_SIZE, 0);
    }

    /**
     * Returns the files listing output on a remotePath
     *
     * @param remotePath Always relative to rootDir
     * @return list of FileInfo
     * @throws IOException       Generally a network error
     * @throws NotFoundException No file at target
     */
    @Override
    public List<FileInfo> ls(@NotNull String remotePath, long limit, long offset) throws IOException, NotFoundException {
        remotePath = FilenameUtils.normalize(remotePath);
        remotePath = StringUtils.isEmpty(remotePath) ? "/" : remotePath;
        long count = Math.min(limit, Constants.MAX_LISTING_SIZE);
        long startIdx = Math.max(offset, 0);

        List<FileInfo> filesList = new ArrayList<>();
        List<DirEntry> dirEntries = new ArrayList<>();
        Path absolutePath = Paths.get(rootDir, remotePath);
        Path rootDirPath = Paths.get(rootDir);
        SSHSftpClient sftpClient = connectionHolder.getSftpClient();
        try {
            Attributes attributes = sftpClient.stat(absolutePath.toString());
            if (attributes.isDirectory()) {
                Iterable<DirEntry> tmp = sftpClient.readDir(absolutePath.toString());
                tmp.forEach(dirEntries::add);
            } else {
                DirEntry entry = new DirEntry(remotePath, remotePath, attributes);
                dirEntries.add(entry);
            }
        } catch (IOException e) {
            if (e.getMessage().toLowerCase().contains("no such file")) {
                String msg = Utils.getMsg("FILES_CLIENT_SSH_NOT_FOUND", oboTenant, oboUser, systemId, username, host, remotePath);
                throw new NotFoundException(msg);
            } else {
                String msg = Utils.getMsg("FILES_CLIENT_SSH_OP_ERR1", oboTenant, oboUser, "ls", systemId, username, host, remotePath, e.getMessage());
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
                fileInfo.setType("dir");
            } else {
                fileInfo.setType("file");
            }
            fileInfo.setOwner(String.valueOf(attrs.getUserId()));
            fileInfo.setGroup(String.valueOf(attrs.getGroupId()));
            fileInfo.setNativePermissions(FileStatInfo.getPermsFromInt(attrs.getPermissions()));
            //Path should be relative to rootDir
            Path tmpFilePath = Paths.get(rootDir, entry.getFilename());
            if (tmpFilePath.equals(absolutePath)) {
                String thePath = StringUtils.prependIfMissing(entryPath.toString(), "/");
                fileInfo.setPath(thePath);
            } else {
                fileInfo.setPath(Paths.get("/", remotePath, entryPath.toString()).toString());
            }

            filesList.add(fileInfo);
        }
        filesList.sort(Comparator.comparing(FileInfo::getName));
        return filesList.stream().skip(startIdx).limit(count).collect(Collectors.toList());
    }


    /**
     * Creates a directory on a remotePath relative to rootDir
     *
     * @throws IOException
     */
    @Override
    public void mkdir(@NotNull String remotePath) throws IOException {
        remotePath = FilenameUtils.normalize(remotePath);
        Path remote = Paths.get(rootDir, remotePath);
        Path rootDirPath = Paths.get(rootDir);
        Path relativePath = rootDirPath.relativize(remote);
        SSHSftpClient sftpClient = connectionHolder.getSftpClient();
        try {

            Path tmpPath = Paths.get(rootDir);
            for (Path part: relativePath) {
                tmpPath = tmpPath.resolve(part);
                try {
                    sftpClient.mkdir(tmpPath.toString());
                } catch (SftpException ignored) {}
            }
        } catch (IOException e) {
            if (e.getMessage().toLowerCase().contains("no such file")) {
                String msg = Utils.getMsg("FILES_CLIENT_SSH_NOT_FOUND", oboTenant, oboUser, systemId, username, host, remotePath);
                throw new NotFoundException(msg);
            } else {
                String msg = Utils.getMsg("FILES_CLIENT_SSH_OP_ERR1", oboTenant, oboUser, "mkdir", systemId, username, host, remotePath, e.getMessage());
                throw new IOException(msg, e);
            }
        } finally {
            sftpClient.close();
            connectionHolder.returnSftpClient(sftpClient);
        }
    }


    private void insertOrAppend(@NotNull String path, @NotNull InputStream fileStream, @NotNull Boolean append) throws IOException, NotFoundException {
        path = FilenameUtils.normalize(path);
        Path absolutePath = Paths.get(rootDir, path).normalize();
        Path relativeRemotePath = Paths.get(StringUtils.stripStart(path, "/")).normalize();
        Path parentPath = relativeRemotePath.getParent();
        SSHSftpClient sftpClient = connectionHolder.getSftpClient();
        try (fileStream) {
            if (parentPath != null) this.mkdir(parentPath.toString());
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


    @Override
    public void insert(@NotNull String remotePath, @NotNull InputStream fileStream) throws IOException {
        insertOrAppend(remotePath, fileStream, false);
    }


    /**
     * Rename/move oldPath to newPath
     *
     * @param oldPath current location
     * @param newPath desired location
     * @return
     * @throws IOException       Network errors generally
     * @throws NotFoundException No file found at target
     */
    @Override
    public void move(@NotNull String oldPath, @NotNull String newPath) throws IOException, NotFoundException {
        oldPath = FilenameUtils.normalize(oldPath);
        newPath = FilenameUtils.normalize(newPath);
        Path absoluteOldPath = Paths.get(rootDir, oldPath);
        Path absoluteNewPath = Paths.get(rootDir, newPath);
        SSHSftpClient sftpClient = connectionHolder.getSftpClient();
        try {
            sftpClient.rename(absoluteOldPath.toString(), absoluteNewPath.toString());
        } catch (IOException e) {
            if (e.getMessage().toLowerCase().contains("no such file")) {
                String msg = Utils.getMsg("FILES_CLIENT_SSH_NOT_FOUND", oboTenant, oboUser, systemId, username, host, oldPath);
                throw new NotFoundException(msg);
            } else {
                String msg = Utils.getMsg("FILES_CLIENT_SSH_OP_ERR2", oboTenant, oboUser, "move", systemId, username, host, oldPath, newPath, e.getMessage());
                throw new IOException(msg, e);
            }
        }  finally {
            sftpClient.close();
            connectionHolder.returnSftpClient(sftpClient);
        }
    }


    /**
     * @param currentPath Relative to roodDir
     * @param newPath     Relative to rootDir
     * @return
     * @throws IOException
     * @throws NotFoundException
     */
    @Override
    public void copy(@NotNull String currentPath, @NotNull String newPath) throws IOException, NotFoundException {
        currentPath = FilenameUtils.normalize(currentPath);
        newPath = FilenameUtils.normalize(newPath);
        Path absoluteCurrentPath = Paths.get(rootDir, currentPath);
        Path absoluteNewPath = Paths.get(rootDir, newPath);
        Path targetParentPath = absoluteNewPath.getParent();

        //This will throw a NotFoundException if source is not there
        this.ls(currentPath);
        SSHExecChannel channel = connectionHolder.getExecChannel();
        try  {
            Map<String, String> args = new HashMap<>();
            args.put("targetParentPath", targetParentPath.toString());
            args.put("source", absoluteCurrentPath.toString());
            args.put("target", absoluteNewPath.toString());

            CommandLine cmd = new CommandLine("mkdir");
            cmd.addArgument("-p");
            cmd.addArgument("${targetParentPath}");
            cmd.addArgument(";");
            cmd.addArgument("cp");
            cmd.addArgument("${source}");
            cmd.addArgument("${target}");
            cmd.setSubstitutionMap(args);
            String toExecute = String.join(" ", cmd.toStrings());
            channel.execute(toExecute);
        } catch (TapisException e) {
            if (e.getMessage().toLowerCase().contains("no such file")) {
                String msg = Utils.getMsg("FILES_CLIENT_SSH_NOT_FOUND", oboTenant, oboUser, systemId, username, host, currentPath);
                throw new NotFoundException(msg);
            } else {
                String msg = Utils.getMsg("FILES_CLIENT_SSH_OP_ERR2", oboTenant, oboUser, "copy", systemId, username, host, currentPath, newPath, e.getMessage());
                throw new IOException(msg, e);
            }
        } finally {
            connectionHolder.returnExecChannel(channel);
        }

    }


    private void recursiveDelete(SSHSftpClient sftpClient, String path) throws IOException {
        path = FilenameUtils.normalize(path);
        path = StringUtils.isEmpty(path) ? "/" : path;
        String cleanedPath = Paths.get(rootDir, path).normalize().toString();
        List<FileInfo> files = ls(path);
        FileStatInfo attributes = getStatInfo(path, false);
        for (FileInfo entry : files) {
            if ((!entry.getName().equals(".")) && (!entry.getName().equals("..")) && (entry.isDir())) {
                Path tmpPath = Paths.get(path, entry.getName()).normalize();
                recursiveDelete(sftpClient, tmpPath.toString());
            } else {
                //The path from the file listing is relative to rootDir
                Path tmpPath = Paths.get(rootDir, entry.getPath());
                sftpClient.remove(tmpPath.normalize().toString());
            }
        }
        if (!cleanedPath.equals(rootDir) && attributes.isDir()) {
            sftpClient.rmdir(cleanedPath);
        }
    }

    @Override
    public void delete(@NotNull String path) throws IOException {
        path = FilenameUtils.normalize(path);
        SSHSftpClient sftpClient = connectionHolder.getSftpClient();
        try  {
            recursiveDelete(sftpClient, path);
        } catch (IOException e) {
            if (e.getMessage().toLowerCase().contains("no such file")) {
                String msg = Utils.getMsg("FILES_CLIENT_SSH_NOT_FOUND", oboTenant, oboUser, systemId, username, host, path);
                throw new NotFoundException(msg);
            } else {
                String msg = Utils.getMsg("FILES_CLIENT_SSH_OP_ERR1", oboTenant, oboUser, "delete", systemId, username, host, path, e.getMessage());
                throw new IOException(msg, e);
            }
        } finally {
            sftpClient.close();
            connectionHolder.returnSftpClient(sftpClient);
        }
    }

    @Override
    public InputStream getStream(@NotNull String path) throws IOException {
        Path absPath = Paths.get(rootDir, path);
        SSHSftpClient sftpClient = connectionHolder.getSftpClient();
        try {
            InputStream inputStream = sftpClient.read(absPath.toString());
            //TapisSSHInputStream closes the sftp connection after reading completes
            return new TapisSSHInputStream(inputStream, connectionHolder, sftpClient);
        } catch (IOException e) {
            connectionHolder.returnSftpClient(sftpClient);
            if (e.getMessage().toLowerCase().contains("no such file")) {
                String msg = Utils.getMsg("FILES_CLIENT_SSH_NOT_FOUND", oboTenant, oboUser, systemId, username, host, path);
                throw new NotFoundException(msg);
            } else {
                String msg = Utils.getMsg("FILES_CLIENT_SSH_OP_ERR1", oboTenant, oboUser, "getStream", systemId, username, host, path, e.getMessage());
                log.error(msg, e);
                throw new IOException(msg, e);
            }
        }
    }

    @Override
    public void download(String path) {
        // TODO Auto-generated method stub
    }

    @Override
    public InputStream getBytesByRange(@NotNull String path, long startByte, long count) throws IOException {
        Path absPath = Paths.get(rootDir, path).normalize();
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
            if (e.getMessage().toLowerCase().contains("no such file")) {
                String msg = Utils.getMsg("FILES_CLIENT_SSH_NOT_FOUND", oboTenant, oboUser, systemId, username, host, path);
                throw new NotFoundException(msg);
            } else {
                String msg = Utils.getMsg("FILES_CLIENT_SSH_OP_ERR1", oboTenant, oboUser, "getBytesByRange", systemId, username, host, path, e.getMessage());
                log.error(msg, e);
                throw new IOException(msg, e);
            }
        }
    }

    @Override
    public void putBytesByRange(String path, InputStream byteStream, long startByte, long endByte) throws IOException {

    }

    @Override
    public void append(@NotNull String path, @NotNull InputStream byteStream) throws IOException {
        insertOrAppend(path, byteStream, true);
    }

    public List<FileInfo> lsRecursive(String basePath) throws IOException, NotFoundException {
        List<FileInfo> filesList = new ArrayList<>();
        listDirectoryRec(basePath, filesList);
        return filesList;
    }

    // ------------------------------
    // Native Linux Utility Methods
    // ------------------------------

    /**
     * Returns the statInfo result for a remotePath
     *
     * @param remotePath - path to check
     * @return statInfo result
     * @throws IOException       Generally a network error
     * @throws NotFoundException No file at target
     */
    @Override
    public FileStatInfo getStatInfo(@NotNull String remotePath, boolean followLinks)
        throws IOException, NotFoundException {
        FileStatInfo statInfo;
        String opName = followLinks ? "lstat" : "stat";
        // Path should have already been normalized and checked but for safety and security do it
        //   again here. FilenameUtils.normalize() is expected to protect against escaping via ../..
        String safePath = getNormalizedPath(remotePath);
        Path absolutePath = Paths.get(rootDir, safePath);
        String absolutePathStr = absolutePath.normalize().toString();
        Attributes sftpAttrs;
        SSHSftpClient sftpClient = connectionHolder.getSftpClient();
        try {
            // If path is a symbolic link then stat gives info for the link target, lstat gives info for the link
            sftpAttrs = followLinks ? sftpClient.stat(absolutePathStr) : sftpClient.lstat(absolutePathStr);
        } catch (IOException e) {
            if (e.getMessage().toLowerCase().contains("no such file")) {
                String msg = Utils.getMsg("FILES_CLIENT_SSH_NOT_FOUND", oboTenant, oboUser, systemId, username, host, remotePath);
                throw new NotFoundException(msg);
            } else {
                String msg = Utils.getMsg("FILES_CLIENT_SSH_OP_ERR1", oboTenant, oboUser, opName, systemId, username, host,
                    remotePath, e.getMessage());
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
     * @param remotePath - target of operation
     * @param permsStr   - perms as octal (000 through 777)
     * @param recursive  - add -R for recursive
     */
    @Override
    public NativeLinuxOpResult linuxChmod(@NotNull String remotePath, @NotNull String permsStr, boolean recursive)
        throws TapisException, IOException, NotFoundException {
        String opName = "chmod";

        // Parse and validate the chmod perms argument
        try {
            int permsInt = Integer.parseInt(permsStr, 8);
            // Check that value is in allowed range
            if (permsInt > MAX_PERMS_INT || permsInt < 0) {
                String msg = Utils.getMsg("FILES_CLIENT_SSH_CHMOD_PERMS", oboTenant, oboUser, systemId, username, host,
                    remotePath, permsStr);
                throw new TapisException(msg);
            }
        } catch (NumberFormatException e) {
            String msg = Utils.getMsg("FILES_CLIENT_SSH_CHMOD_ERR", oboTenant, oboUser, systemId, username, host,
                remotePath, permsStr, e.getMessage());
            throw new TapisException(msg, e);
        }

        // Run the command
        return runLinuxChangeOp(opName, permsStr, remotePath, recursive);
    }

    @Override
    public NativeLinuxOpResult linuxChown(@NotNull String remotePath, @NotNull String newOwner, boolean recursive)
        throws TapisException, IOException, NotFoundException {
        String opName = "chown";

        // Validate that owner is valid linux user name
        if (!USER_REGEX.matcher(newOwner).matches()) {
            String msg = Utils.getMsg("FILES_CLIENT_SSH_LINUXOP_USRGRP", oboTenant, oboUser, systemId, username, host,
                remotePath, opName, newOwner);
            throw new TapisException(msg);
        }

        // Run the command
        return runLinuxChangeOp(opName, newOwner, remotePath, recursive);
    }

    @Override
    public NativeLinuxOpResult linuxChgrp(@NotNull String remotePath, @NotNull String newGroup, boolean recursive)
        throws TapisException, IOException, NotFoundException {
        String opName = "chgrp";

        // Validate that group is valid linux group name
        if (!USER_REGEX.matcher(newGroup).matches()) {
            String msg = Utils.getMsg("FILES_CLIENT_SSH_LINUXOP_USRGRP", oboTenant, oboUser, systemId, username, host,
                remotePath, opName, newGroup);
            throw new TapisException(msg);
        }
        // Run the command
        return runLinuxChangeOp(opName, newGroup, remotePath, recursive);
    }


    /**
     * Use Apache's FileNameUtils to get a normalized path
     *
     * @param remotePath - path to normalize
     * @return normalized path
     * @throws IllegalArgumentException - if path normalizes to null
     */
    private String getNormalizedPath(String remotePath) throws IllegalArgumentException {
        // FilenameUtils.normalize() is expected to protect against escaping via ../..
        String safePath = FilenameUtils.normalize(remotePath);
        if (safePath == null) {
           safePath = "/";
        }
        return safePath;
    }

    /**
     * Run one of the linux change operations: chmod, chown, chgrp
     *
     * @param opName     - Operation to execute
     * @param arg1       - argument for operation (perms as octal, new owner, new group)
     * @param remotePath - target of operation
     * @param recursive  - add -R for recursive
     */
    private NativeLinuxOpResult runLinuxChangeOp(String opName, String arg1, String remotePath, boolean recursive)
        throws TapisException, IOException, NotFoundException {
        // Make sure we have a valid first argument
        if (StringUtils.isBlank(arg1)) {
            String msg = Utils.getMsg("FILES_CLIENT_SSH_LINUXOP_NOARG", oboTenant, oboUser, systemId, username, host,
                remotePath, opName);
            throw new TapisException(msg);
        }

        // Path should have already been normalized and checked but for safety and security do it
        //   again here. FilenameUtils.normalize() is expected to protect against escaping via ../..
        String safePath = getNormalizedPath(remotePath);
        Path absolutePath = Paths.get(rootDir, safePath);
        String absolutePathStr = absolutePath.normalize().toString();

        // Check that path exists. This will throw a NotFoundException if path is not there
        this.ls(safePath);

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
                remotePath, opName, exitCode, stdOut.toString(), stdErr.toString());
            log.warn(msg);
        }
        connectionHolder.returnExecChannel(cmdRunner);
        return new NativeLinuxOpResult(cmdStr, exitCode, String.valueOf(stdOut), String.valueOf(stdErr));

    }
}
