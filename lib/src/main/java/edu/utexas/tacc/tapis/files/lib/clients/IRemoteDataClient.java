package edu.utexas.tacc.tapis.files.lib.clients;

import edu.utexas.tacc.tapis.files.lib.models.FileInfo;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;

public interface IRemoteDataClient {

    List<FileInfo> ls(String remotePath) throws IOException;
    FileInfo insert(String remotePath, InputStream fileStream) throws IOException;
    void move(String srcSystem, String srcPath, String destSystem, String destPath);
    void rename();
    void copy();
    void delete();
    void getStream();
    void download();
    void connect();
    void disconnect();
}
