package edu.utexas.tacc.tapis.files.lib.clients;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.commons.exec.CommandLine;
import org.apache.commons.io.FilenameUtils;
import org.jetbrains.annotations.NotNull;
import javax.ws.rs.NotFoundException;

import org.apache.commons.lang3.NotImplementedException;
import org.apache.commons.lang3.StringUtils;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.jcraft.jsch.ChannelExec;
import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.ChannelSftp.LsEntry;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.SftpATTRS;
import com.jcraft.jsch.SftpException;

import edu.utexas.tacc.tapis.files.lib.utils.Utils;
import edu.utexas.tacc.tapis.files.lib.utils.PathUtils;

import edu.utexas.tacc.tapis.files.lib.models.FileInfo;
import edu.utexas.tacc.tapis.files.lib.utils.Constants;
import edu.utexas.tacc.tapis.shared.ssh.SSHConnection;
import edu.utexas.tacc.tapis.shared.ssh.TapisJSCHInputStream;
import edu.utexas.tacc.tapis.systems.client.gen.model.TSystem;


/**
 * This class is the entry point to sile operations over SSH with Tapis.
 * All path parameters as inputs to methods are assumed to be relative to the rootDir
 * of the system unless otherwise specified.
 */
public class SSHDataClient implements IRemoteDataClient {

    private final Logger log = LoggerFactory.getLogger(SSHDataClient.class);

    public String getOboTenant() { return oboTenant; }
    public String getOboUser() { return oboUser; }
    public String getSystemId() { return systemId; }
    private final String oboTenant;
    private final String oboUser;

    private final String host;
    private final String username;
    private final SSHConnection sshConnection;
    private final String rootDir;
    private final String systemId;
    private static final int MAX_LISTING_SIZE = Constants.MAX_LISTING_SIZE;

    public SSHDataClient(@NotNull String oboTenant1, @NotNull String oboUser1, @NotNull TSystem sys, SSHConnection sshCon) {
        oboTenant = oboTenant1;
        oboUser = oboUser1;
        String rdir = sys.getRootDir();
        rdir = StringUtils.isBlank(rdir) ? "/" : rdir;
        rootDir = Paths.get(rdir).normalize().toString();
        host = sys.getHost();
        username = sys.getEffectiveUserId();
        systemId = sys.getId();
        sshConnection = sshCon;
    }

    @Override
    public void makeBucket(String name) throws IOException {
        String msg = Utils.getMsg("FILES_CLIENT_SSH_NO_SUPPORT", oboTenant, oboUser, "makeBucket", systemId, name);
        throw new NotImplementedException(msg);
    }

    public List<FileInfo> lsRecursive(String basePath) throws IOException, NotFoundException {
        List<FileInfo> filesList = new ArrayList<>();
        listDirectoryRec(basePath, filesList);
        return filesList;
    }

    private void listDirectoryRec(String basePath, List<FileInfo> listing) throws IOException, NotFoundException{
        List<FileInfo> currentListing = this.ls(basePath);
        listing.addAll(currentListing);
        for (FileInfo fileInfo: currentListing) {
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
     * @param remotePath
     * @return list of FileInfo
     * @throws IOException Generally a network error
     * @throws NotFoundException No file at target
     */
    @Override
    public List<FileInfo> ls(@NotNull String remotePath, long limit, long offset) throws IOException, NotFoundException {
        long count = Math.min(limit, Constants.MAX_LISTING_SIZE);
        long startIdx = Math.max(offset, 0);

        List<FileInfo> filesList = new ArrayList<>();
        List<?> filelist;
        Path absolutePath = Paths.get(rootDir, remotePath);

        ChannelSftp channelSftp = openAndConnectSFTPChannel();

        try {
            filelist = channelSftp.ls(absolutePath.toString());
        } catch (SftpException e) {
            if (e.getMessage().toLowerCase().contains("no such file")) {
                String msg = Utils.getMsg("FILES_CLIENT_SSH_NOT_FOUND", oboTenant, oboUser, systemId, username, host, remotePath);
                throw new NotFoundException(msg);
            } else {
                String msg = Utils.getMsg("FILES_CLIENT_SSH_OP_ERR1", oboTenant, oboUser, "ls", systemId, username, host, remotePath, e.getMessage());
                throw new IOException(msg, e);
            }
        } finally {
            sshConnection.returnChannel(channelSftp);
        }

        // For each entry in the fileList received, get the fileInfo object
        for (int i = 0; i < filelist.size(); i++) {
            LsEntry entry = (LsEntry) filelist.get(i);
            // Get the file attributes
            SftpATTRS attrs = entry.getAttrs();
            FileInfo fileInfo = new FileInfo();
            // Ignore filename . and ..
            if (entry.getFilename().equals(".") || entry.getFilename().equals("..")) {
                continue;
            }
            fileInfo.setName(entry.getFilename());
            // Obtain the modified time for the file from the attribute
            // and set lastModified field
            DateTimeFormatter dateTimeformatter = DateTimeFormatter.ofPattern("EEE MMM d HH:mm:ss zzz uuuu", Locale.US);
            ZonedDateTime lastModified = ZonedDateTime.parse(attrs.getMtimeString(), dateTimeformatter);
            fileInfo.setLastModified(lastModified.toInstant());
            fileInfo.setSize(attrs.getSize());
            Path tmpPath = Paths.get(entry.getFilename());
            fileInfo.setMimeType(Files.probeContentType(tmpPath));
            if (attrs.isDir()) {
                fileInfo.setType("dir");
            } else {
                fileInfo.setType("file");
            }
            fileInfo.setOwner(String.valueOf(attrs.getUId()));
            fileInfo.setGroup(String.valueOf(attrs.getGId()));
            fileInfo.setPermissions(attrs.getPermissionsString());
            //TODO: This path munging is tricky, but it seems to work as far as listings are concerned
            Path fullPath;
            if (absolutePath.getFileName().equals(Paths.get(entry.getFilename()))) {
                fullPath = Paths.get(remotePath);
            } else {
                fullPath = Paths.get(remotePath).resolve(entry.getFilename());
            }

            fileInfo.setPath(fullPath.toString());
            filesList.add(fileInfo);
        }
        filesList.sort((a, b)->{
            return a.getName().compareTo(b.getName());
        });
        return filesList.stream().skip(startIdx).limit(count).collect(Collectors.toList());
    }


    /**
     * Creates a directory on a remotePath relative to rootDir
     *
     * @throws IOException
     */
    @Override
    public void mkdir(@NotNull String remotePath) throws IOException {

        Path remote = Paths.get(remotePath);
        ChannelSftp channelSftp = openAndConnectSFTPChannel();
        try {
            channelSftp.cd(rootDir);
            for (Path part : remote) {
                try {
                    channelSftp.cd(part.toString());
                } catch (SftpException ex) {
                    channelSftp.mkdir(part.toString());  // do we need to set some permission on the directory??
                    channelSftp.cd(part.toString());
                }
            }
        } catch (SftpException e) {
            if (e.getMessage().toLowerCase().contains("no such file")) {
                String msg = Utils.getMsg("FILES_CLIENT_SSH_NOT_FOUND", oboTenant, oboUser, systemId, username, host, remotePath);
                throw new NotFoundException(msg);
            } else {
                String msg = Utils.getMsg("FILES_CLIENT_SSH_OP_ERR1", oboTenant, oboUser, "mkdir", systemId, username, host, remotePath, e.getMessage());
                throw new IOException(msg, e);
            }
        } finally {
           sshConnection.returnChannel(channelSftp);
        }

    }


    private void insertOrAppend(@NotNull String path, @NotNull InputStream fileStream, @NotNull Boolean append) throws IOException, NotFoundException {
        Path absolutePath = Paths.get(rootDir, path).normalize();
        Path relativeRemotePath = Paths.get(StringUtils.stripStart(path, "/")).normalize();
        Path parentPath = relativeRemotePath.getParent();

        ChannelSftp channelSftp = openAndConnectSFTPChannel();

        int channelOptions = append ? ChannelSftp.APPEND : ChannelSftp.OVERWRITE;

        try {
            if (parentPath != null) this.mkdir(parentPath.toString());
            channelSftp.cd(rootDir);
            if (parentPath != null) channelSftp.cd(parentPath.toString());
            channelSftp.put(fileStream, absolutePath.getFileName().toString(), channelOptions);
        } catch (SftpException ex) {
            String msg = Utils.getMsg("FILES_CLIENT_SSH_OP_ERR1", oboTenant, oboUser, "insertOrAppend", systemId, username, host, path, ex.getMessage());
            throw new IOException(msg, ex);
        } finally {
            sshConnection.returnChannel(channelSftp);
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
     * @throws IOException Network errors generally
     * @throws NotFoundException No file found at target
     */
    @Override
    public void move(@NotNull String oldPath, @NotNull String newPath) throws IOException, NotFoundException {

        Path absoluteOldPath = Paths.get(rootDir, oldPath);
        Path absoluteNewPath = Paths.get(rootDir, newPath);

        ChannelSftp channelSftp = openAndConnectSFTPChannel();

        try {
            channelSftp.rename(absoluteOldPath.toString(), absoluteNewPath.toString());
        } catch (SftpException e) {
            if (e.getMessage().toLowerCase().contains("no such file")) {
                String msg = Utils.getMsg("FILES_CLIENT_SSH_NOT_FOUND", oboTenant, oboUser, systemId, username, host, oldPath);
                throw new NotFoundException(msg);
            } else {
                String msg = Utils.getMsg("FILES_CLIENT_SSH_OP_ERR2", oboTenant, oboUser, "move", systemId, username, host, oldPath, newPath, e.getMessage());
                throw new IOException(msg, e);
            }
        } finally {
            sshConnection.returnChannel(channelSftp);
        }
    }


    /**
     * @param currentPath Relative to roodDir
     * @param newPath Relative to rootDir
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
        ChannelExec channel = openCommandChannel();
        try {
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
            channel.setCommand(toExecute);
            channel.connect();
        } catch (JSchException e) {
            if (e.getMessage().toLowerCase().contains("no such file")) {
                String msg = Utils.getMsg("FILES_CLIENT_SSH_NOT_FOUND", oboTenant, oboUser, systemId, username, host, currentPath);
                throw new NotFoundException(msg);
            } else {
                String msg = Utils.getMsg("FILES_CLIENT_SSH_OP_ERR2", oboTenant, oboUser, "copy", systemId, username, host, currentPath, newPath, e.getMessage());
                throw new IOException(msg, e);
            }
        } finally {
            sshConnection.returnChannel(channel);
        }

    }


    /**
     * Path is relative to rootDir
     *
     * @param channelSftp
     * @param path
     * @throws SftpException
     */
    private void recursiveDelete(ChannelSftp channelSftp, String path) throws SftpException {
        String cleanedPath = Paths.get(path).normalize().toString();

        SftpATTRS attrs = channelSftp.stat(cleanedPath);
        if (attrs.isDir()) {
            Collection<LsEntry> files = channelSftp.ls(path);
            if (files != null && !files.isEmpty()) {
                for (LsEntry entry : files) {
                    if ((!entry.getFilename().equals(".")) && (!entry.getFilename().equals(".."))) {
                        recursiveDelete(channelSftp, path + "/" + entry.getFilename());
                    }
                }
            }
            channelSftp.rmdir(path);
        } else {
            channelSftp.rm(path);
        }
    }

    private void clearRootDir(ChannelSftp channel) {
        Collection<LsEntry> files;
        try {
            files = channel.ls(rootDir);
            if (files != null && files.size() > 0) {
                for (LsEntry entry : files) {
                    if ((!entry.getFilename().equals(".")) && (!entry.getFilename().equals(".."))) {
                        recursiveDelete(channel, rootDir + "/" + entry.getFilename());
                    }
                }
            }
        } catch (SftpException ex) {
            ex.printStackTrace();
        }
    }


    @Override
    public void delete(@NotNull String path) throws IOException {
        Path absPath = Paths.get(rootDir, path);
        ChannelSftp channelSftp = openAndConnectSFTPChannel();
        // If the path is "/", then just clear everything out of rootDir?
        try {
            if (absPath.equals(Paths.get(rootDir))) {
                clearRootDir(channelSftp);
            } else {
                recursiveDelete(channelSftp, absPath.toString());
            }
        } catch (SftpException e) {
            if (e.getMessage().toLowerCase().contains("no such file")) {
                String msg = Utils.getMsg("FILES_CLIENT_SSH_NOT_FOUND", oboTenant, oboUser, systemId, username, host, path);
                throw new NotFoundException(msg);
            } else {
                String msg = Utils.getMsg("FILES_CLIENT_SSH_OP_ERR1", oboTenant, oboUser, "delete", systemId, username, host, path, e.getMessage());
                throw new IOException(msg, e);
            }
        } finally {
            sshConnection.returnChannel(channelSftp);
        }
    }

    @Override
    public InputStream getStream(@NotNull String path) throws IOException {
        Path absPath = Paths.get(rootDir, path);
        ChannelSftp channelSftp = openAndConnectSFTPChannel();
        InputStream inputStream = null;
        try {
            inputStream = channelSftp.get(absPath.toString());
            TapisJSCHInputStream shellInputStream = new TapisJSCHInputStream(inputStream, sshConnection, channelSftp);
            return shellInputStream;
        } catch (SftpException e) {
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
    public void connect() throws IOException {
        try {
            if (!sshConnection.getSession().isConnected()) {
                sshConnection.getSession().connect();
            }
        } catch (JSchException ex) {
            String msg = Utils.getMsg("FILES_CLIENT_SSH_CONN_ERR1", oboTenant, oboUser, systemId, username, host, ex.getMessage());
            log.error(msg);
            throw new IOException(msg, ex);
        }
    }

    @Override
    public void disconnect() {
        sshConnection.closeSession();
    }

    @Override
    public InputStream getBytesByRange(@NotNull String path, long startByte, long count) throws IOException {
        Path absPath = Paths.get(rootDir, path).normalize();
        ChannelExec channel = openCommandChannel();
        //TODO: Really need to sanitize this command
        //TODO: Really do
        try {
            String command = String.format("dd if=%s ibs=1 skip=%s count=%s", absPath.toString(), startByte, count);
            channel.setCommand(command);
            InputStream commandOutput = channel.getInputStream();
            TapisJSCHInputStream shellInputStream = new TapisJSCHInputStream(commandOutput, sshConnection, channel);
            channel.connect();
            return shellInputStream;
        } catch (JSchException e) {
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

    private ChannelSftp openAndConnectSFTPChannel() throws IOException {
        String CHANNEL_TYPE = "sftp";
        ChannelSftp channel = (ChannelSftp) sshConnection.createChannel(CHANNEL_TYPE);

        // log.info("Current channel count is {}", sshConnection.getChannelCount());
        //TODO: This will fail with a strange error if the max channels per connection limit is reached.
        //TODO: Need to find a way to pool channels?
        try {
            channel.connect(10*1000);
            return channel;
        } catch (JSchException e) {
            String msg = Utils.getMsg("FILES_CLIENT_SSH_CONN_ERR2", oboTenant, oboUser, systemId, username, host, e.getMessage());
            log.error(msg);
            sshConnection.returnChannel(channel);
            throw new IOException(msg, e);
        }
    }

    private ChannelExec openCommandChannel() throws IOException {
        String CHANNEL_TYPE = "exec";
        return (ChannelExec) sshConnection.createChannel(CHANNEL_TYPE);
    }
}
