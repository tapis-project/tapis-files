package edu.utexas.tacc.tapis.files.lib.clients;

import java.io.*;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

import javax.validation.constraints.NotNull;
import javax.ws.rs.NotFoundException;

import com.jcraft.jsch.*;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.jcraft.jsch.ChannelSftp.LsEntry;

import edu.utexas.tacc.tapis.files.lib.kernel.ProgressMonitor;
import edu.utexas.tacc.tapis.files.lib.kernel.SSHConnection;
import edu.utexas.tacc.tapis.files.lib.models.FileInfo;
import edu.utexas.tacc.tapis.systems.client.gen.model.TSystem;

public class SSHDataClient implements IRemoteDataClient {

    private final Logger log = LoggerFactory.getLogger(SSHDataClient.class);
    private final String host;
    private final int port;
    private final String username;
    private final String password;
    private final TSystem.DefaultAccessMethodEnum accessMethod;
    private SSHConnection sshConnection;
    private final String rootDir;
    private final String systemId;

    public SSHDataClient(TSystem system) {
        host = system.getHost();
        port = system.getPort();
        username = system.getEffectiveUserId();
        password = system.getAccessCredential().getPassword();
        accessMethod = system.getDefaultAccessMethod();
        rootDir = Paths.get(system.getRootDir()).normalize().toString();
        systemId = system.getName();
    }

    /**
     * Returns the files listing output on a remotePath
     *
     * @param remotePath
     * @return list of FileInfo
     * @throws IOException
     * @throws NotFoundException
     */
    @Override
    public List<FileInfo> ls(@NotNull String remotePath) throws IOException, NotFoundException {

        List<FileInfo> filesList = new ArrayList<>();
        List<?> filelist;
        Path absolutePath = Paths.get(rootDir, remotePath);

        ChannelSftp channelSftp = openAndConnectSFTPChannel();

        try {
            log.debug("SSH DataClient Listing ls path: " + absolutePath.toString());
            filelist = channelSftp.ls(absolutePath.toString());
        } catch (SftpException e) {
            if (e.getMessage().toLowerCase().contains("no such file")) {
                String msg = "SSH_DATACLIENT_FILE_NOT_FOUND" + " for user: "
                        + username + ", on host: "
                        + host + ", path: " + absolutePath.toString() + ": " + e.getMessage();
                log.error(msg, e);
                throw new NotFoundException(msg);
            } else {
                String msg = "SSH_DATACLIENT_FILE_LISTING_ERROR in method " + this.getClass().getName() + " for user: "
                        + username + ", on host: "
                        + host + ", path :" + absolutePath.toString() + " :  " + e.getMessage();
                log.error(msg, e);
                throw new IOException(msg, e);
            }
        } finally {
            sshConnection.closeChannel(channelSftp);
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
            //Check if the entry is a directory or a file
            if (attrs.isReg()) {
                Path fullPath = Paths.get(remotePath, entry.getFilename());
                fileInfo.setPath(fullPath.toString());
            } else {
                fileInfo.setPath(remotePath);
            }
            filesList.add(fileInfo);
        }
        return filesList;
    }


    /**
     * Creates a directory on a remotePath relative to rootDir
     *
     * @return
     * @throws IOException
     * @throws NotFoundException
     */
    @Override
    public void mkdir(@NotNull String remotePath) throws IOException {

        Path remote = Paths.get(remotePath);
        ChannelSftp channelSftp = openAndConnectSFTPChannel();

        try {
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
                String msg = "SSH_DATACLIENT_FILE_NOT_FOUND" + " for user: "
                        + username + ", on host: "
                        + host + ", path: " + remote.toString() + ": " + e.getMessage();
                log.error(msg, e);
                throw new NotFoundException(msg);
            } else {
                String Msg = "SSH_DATACLIENT_MKDIR_ERROR in method " + this.getClass().getName() + " for user:  "
                        + username + " on host: "
                        + host + " path :" + remote.toString() + " : " + e.toString();
                log.error(Msg, e);

                throw new IOException(Msg, e);
            }
        } finally {
            sshConnection.closeChannel(channelSftp);
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
            log.error("Error inserting file to {}", systemId, ex);
            throw new IOException("Error inserting file into " + systemId);
        } finally {
            sshConnection.closeChannel(channelSftp);
        }
    }


    @Override
    public void insert(@NotNull String remotePath, @NotNull InputStream fileStream) throws IOException {
        insertOrAppend(remotePath, fileStream, false);
    }


    /**
     * Rename/move oldPath to newPath
     *
     * @param oldPath
     * @param newPath
     * @return
     * @throws IOException
     * @throws NotFoundException
     */
    @Override
    public void move(@NotNull String oldPath, @NotNull String newPath) throws IOException, NotFoundException {

        Path absoluteOldPath = Paths.get(rootDir, oldPath);
        Path absoluteNewPath = Paths.get(absoluteOldPath.getParent().toString(), newPath);
        log.debug("SSH DataClient move: oldPath: " + absoluteOldPath.toString());
        log.debug("SSH DataClient move: newPath: " + absoluteNewPath.toString());

        ChannelSftp channelSftp = openAndConnectSFTPChannel();

        try {
            channelSftp.rename(absoluteOldPath.toString(), absoluteNewPath.toString());
        } catch (SftpException e) {
            if (e.getMessage().toLowerCase().contains("no such file")) {
                String msg = "SSH_DATACLIENT_FILE_NOT_FOUND" + " for user: "
                        + username + ", on host: "
                        + host + ", path: " + absoluteOldPath.toString() + ": " + e.getMessage();
                log.error(msg, e);
                throw new NotFoundException(msg);
            } else {
                String Msg = "SSH_DATACLIENT_RENAME_ERROR in method " + this.getClass().getName() + " for user:  "
                        + username + " on host: "
                        + host + " old path :" + absoluteOldPath.toString() + " : " + " newPath: " + absoluteNewPath.toString() + ":" + e.getMessage();
                log.error(Msg, e);
                throw new IOException(Msg, e);
            }
        } finally {
            sshConnection.closeChannel(channelSftp);
        }
    }


    /**
     * @param currentPath
     * @param newPath
     * @return
     * @throws IOException
     * @throws NotFoundException
     */
    @Override
    public void copy(@NotNull String currentPath, @NotNull String newPath) throws IOException, NotFoundException {

        Path absoluteCurrentPath = Paths.get(rootDir, currentPath);
        Path absoluteNewPath = Paths.get(rootDir, newPath);
        log.debug("SSH DataClient copy: currentPath: " + absoluteCurrentPath.toString());
        log.debug("SSH DataClient copy: newPath: " + absoluteNewPath.toString());

        ChannelSftp channelSftp = openAndConnectSFTPChannel();

        try {
            ProgressMonitor progress = new ProgressMonitor();
            channelSftp.put(absoluteCurrentPath.toString(), absoluteNewPath.toString(), progress);

        } catch (SftpException e) {
            if (e.getMessage().toLowerCase().contains("no such file")) {
                String msg = "SSH_DATACLIENT_FILE_NOT_FOUND" + " for user: "
                        + username + ", on host: "
                        + host + ", path: " + absoluteCurrentPath.toString() + ": " + e.getMessage();
                log.error(msg, e);
                throw new NotFoundException(msg);
            } else {
                String Msg = "SSH_DATACLIENT_FILE_COPY_ERROR in method " + this.getClass().getName() + " for user:  "
                        + username + ", on host: "
                        + host + ", from current path: " + absoluteCurrentPath.toString() + " to newPath " + absoluteNewPath.toString() + " : " + e.toString();
                log.error(Msg, e);

                throw new IOException(Msg, e);
            }
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
            if (files != null && files.size() > 0) {
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
        Collection<LsEntry> files = null;
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
            throw new IOException("Could not delete file/folder at path " + absPath.toString());
        } finally {
            sshConnection.closeChannel(channelSftp);
        }
    }

    @Override
    public InputStream getStream(@NotNull String path) throws IOException {
        Path absPath = Paths.get(rootDir, path);
        ChannelSftp channelSftp = openAndConnectSFTPChannel();
        try {
            InputStream inputStream = channelSftp.get(absPath.toString());
            return IOUtils.toBufferedInputStream(inputStream);
        } catch (SftpException ex) {
            log.error("Error retrieving file to {}", systemId, ex);
            throw new IOException("Error retrieving file into " + systemId + " at path " + path);
        } finally {
            sshConnection.closeChannel(channelSftp);
        }
    }

    @Override
    public void download(String path) {
        // TODO Auto-generated method stub

    }

    @Override
    public void connect() throws IOException {

        switch (accessMethod.getValue()) {
            case "PASSWORD": {
                sshConnection = new SSHConnection(host, port, username, password);
                sshConnection.initSession();
                break;
            }
            case "SSH_KEYS":
                //
            default:
                //dosomething
        }

    }

    @Override
    public void disconnect() {
        sshConnection.closeSession();
    }

    @Override
    public InputStream getBytesByRange(@NotNull String path, long startByte, long count) throws IOException {
        Path absPath = Paths.get(rootDir, path);
        StringBuilder outputBuffer = new StringBuilder();
        StringBuilder print = new StringBuilder();
        ChannelExec channel = openAndConnectCommandChannel();

        try {
            String command = String.format("dd if=%s ibs=1 skip=%s count=%s", absPath.toString(), startByte, count);
            InputStream commandOutput = channel.getInputStream();
            channel.setCommand(command);
            channel.setErrStream( System.err, true );
            channel.connect();
            return IOUtils.toBufferedInputStream(commandOutput);
        } catch (JSchException ex) {
            log.error("getBytesByRange error", ex);
            throw new IOException("Could not retrieve bytes");
        } finally {
            sshConnection.closeChannel(channel);
        }
    }

    @Override
    public void putBytesByRange(String path, InputStream byteStream, long startByte, long endByte) throws IOException {

    }

    @Override
    public void append(@NotNull String path, @NotNull InputStream byteStream) throws IOException {
        insertOrAppend(path, byteStream, true);
    }


    /**
     * Opens and connects to SSH SFTP channel for a SSH connection.
     * path parameter is for logging purpose
     *
     * @return ChannelSftp
     * @throws IOException
     */

    private ChannelSftp openAndConnectSFTPChannel() throws IOException {
        String CHANNEL_TYPE = "sftp";
        if (sshConnection.getSession() == null) {
            String msg = "SSH_DATACLIENT_SESSION_NULL_ERROR in method " + this.getClass().getName() + " for user:  "
                    + username + " on destination host: "
                    + host;
            log.error(msg);
            throw new IOException("SSH session error");
        }

        ChannelSftp channel = (ChannelSftp) sshConnection.openChannel(CHANNEL_TYPE);
        sshConnection.connectChannel(channel);
        return channel;
    }

    private ChannelExec openAndConnectCommandChannel() throws IOException {
        String CHANNEL_TYPE = "exec";
        if (sshConnection.getSession() == null) {
            String msg = "SSH_DATACLIENT_SESSION_NULL_ERROR in method " + this.getClass().getName() + " for user:  "
                    + username + " on destination host: "
                    + host;
            log.error(msg);
            throw new IOException("SSH session error");
        }

        return (ChannelExec) sshConnection.openChannel(CHANNEL_TYPE);
    }



}
