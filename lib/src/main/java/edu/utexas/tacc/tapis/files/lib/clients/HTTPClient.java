package edu.utexas.tacc.tapis.files.lib.clients;

import edu.utexas.tacc.tapis.files.lib.models.FileInfo;

import javax.validation.constraints.NotNull;
import javax.ws.rs.NotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import okhttp3.OkHttpClient;


public class HTTPClient implements IRemoteDataClient {


    @Override
    public void makeBucket(String name) throws IOException {

    }

    @Override
    public List<FileInfo> ls(@NotNull String remotePath) throws IOException, NotFoundException {
        return null;
    }

    @Override
    public List<FileInfo> ls(@NotNull String remotePath, long limit, long offset) throws IOException, NotFoundException {
        return null;
    }

    @Override
    public void insert(@NotNull String remotePath, @NotNull InputStream fileStream) throws IOException {

    }

    @Override
    public void mkdir(@NotNull String remotePath) throws IOException, NotFoundException {

    }

    @Override
    public void move(@NotNull String oldPath, @NotNull String newPath) throws IOException, NotFoundException {

    }

    @Override
    public void copy(@NotNull String currentPath, @NotNull String newPath) throws IOException, NotFoundException {

    }

    @Override
    public void delete(@NotNull String path) throws IOException {

    }

    @Override
    public InputStream getStream(@NotNull String path) throws IOException {
        return null;
    }

    @Override
    public InputStream getBytesByRange(@NotNull String path, long startByte, long count) throws IOException {
        return null;
    }

    @Override
    public void putBytesByRange(String path, InputStream byteStream, long startByte, long endByte) throws IOException {

    }

    @Override
    public void append(@NotNull String path, @NotNull InputStream byteStream) throws IOException {

    }

    @Override
    public void download(String path) throws IOException {

    }

    @Override
    public void connect() throws IOException {

    }

    @Override
    public void disconnect() {

    }
}
