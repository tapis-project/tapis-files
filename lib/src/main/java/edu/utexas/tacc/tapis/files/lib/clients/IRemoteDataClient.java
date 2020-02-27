package edu.utexas.tacc.tapis.files.lib.clients;

import edu.utexas.tacc.tapis.files.lib.models.FileInfo;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;

public interface IRemoteDataClient {

    List<FileInfo> ls(String remotePath) throws IOException;
    void insert(String remotePath, InputStream fileStream) throws IOException;
    void mkdir(String remotePath) throws IOException;
    void move(String oldPath, String newPath) throws IOException;
    void copy(String currentPath, String newPath) throws IOException;
    void delete(String path) throws IOException;
    InputStream getStream(String path) throws IOException;
    InputStream getBytesByRange(String path, long startByte, long endByte) throws IOException;
    void putBytesByRange(String path, InputStream byteStream, long startByte, long endByte) throws IOException;
    // Append takes an existing file and adds to the end of it.
    void append(String path, InputStream byteStream) throws IOException;
    void download(String path) throws IOException;
    // Head returns the first N bytes as a UTF-8 encoded byte stream
    InputStream more(String path) throws IOException;
    void connect() throws IOException;
    void disconnect();
}
