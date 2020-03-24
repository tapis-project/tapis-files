package edu.utexas.tacc.tapis.files.lib.clients;

import edu.utexas.tacc.tapis.files.lib.models.FileInfo;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;

public interface IRemoteDataClient {

    List<FileInfo> ls(String remotePath) throws IOException;

    /**
     * Insert will place the entire contents of an InputStream to the location
     * at remotePath.
     * @param remotePath
     * @param fileStream
     * @throws IOException
     */
    void insert(String remotePath, InputStream fileStream) throws IOException;
    void mkdir(String remotePath) throws IOException;
    void move(String oldPath, String newPath) throws IOException;
    void copy(String currentPath, String newPath) throws IOException;
    void delete(String path) throws IOException;

    /**
     * Returns a stream of the entire contents of a file.
     * @param path
     * @return
     * @throws IOException
     */
    InputStream getStream(String path) throws IOException;

    InputStream getBytesByRange(String path, long startByte, long endByte) throws IOException;

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
    void append(String path, InputStream byteStream) throws IOException;
    void download(String path) throws IOException;

    void connect() throws IOException;
    void disconnect();
}
