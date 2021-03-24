package edu.utexas.tacc.tapis.files.lib.clients;

import edu.utexas.tacc.tapis.files.lib.models.FileInfo;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;

import org.jetbrains.annotations.NotNull;
import javax.ws.rs.NotFoundException;

public interface IRemoteDataClient {
    void makeBucket(String name) throws IOException;
    // without limit/offset, just a helper method for convenience
    List<FileInfo> ls(@NotNull String remotePath) throws IOException, NotFoundException;
    List<FileInfo> ls(@NotNull String remotePath, long limit, long offset) throws IOException, NotFoundException;

    /**
     * Insert will place the entire contents of an InputStream to the location
     * at remotePath.
     * @param remotePath
     * @param fileStream
     * @throws IOException
     */
    void insert(@NotNull String remotePath, @NotNull InputStream fileStream) throws IOException;
    void mkdir(@NotNull  String remotePath) throws IOException, NotFoundException;
    void move(@NotNull String oldPath, @NotNull String newPath) throws IOException, NotFoundException;
    void copy(@NotNull String currentPath, @NotNull String newPath) throws IOException, NotFoundException;
    void delete(@NotNull String path) throws IOException;

    /**
     * Returns a stream of the entire contents of a file.
     * @param path
     * @return
     * @throws IOException
     */
    InputStream getStream(@NotNull String path) throws IOException;


    /**
     *
     * @param path path to file
     * @param startByte position of first byte to return
     * @param count Number of bytes returned
     * @return InputStream
     * @throws IOException Generic IO Exception
     */
    InputStream getBytesByRange(@NotNull String path, long startByte, long count) throws IOException;

    /**
     * Not many fielsystems and/or file formats support this type of operation
     * @param path
     * @param byteStream
     * @param startByte
     * @param endByte
     * @throws IOException
     */
    void putBytesByRange(String path, InputStream byteStream, long startByte, long endByte) throws IOException;

    /**
     * Append will take an existing file at location path and
     * append the byteStream to the end of it.
     * @param path
     * @param byteStream
     * @throws IOException
     */
    void append(@NotNull String path, @NotNull InputStream byteStream) throws IOException;
    void download(String path) throws IOException;

    void connect() throws IOException;
    void disconnect();
}
