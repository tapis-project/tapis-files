package edu.utexas.tacc.tapis.files.lib.clients;

import edu.utexas.tacc.tapis.files.lib.models.FileInfo;

import javax.validation.constraints.NotNull;
import javax.ws.rs.NotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.NotActiveException;
import java.util.List;
import okhttp3.OkHttpClient;
import org.apache.commons.lang3.NotImplementedException;


public class HTTPClient implements IRemoteDataClient {


    @Override
    public void makeBucket(String name) throws IOException {
        throw new NotImplementedException("Not implemented for http client");
    }

    @Override
    public List<FileInfo> ls(@NotNull String remotePath) throws IOException, NotFoundException {
        throw new NotImplementedException("Not implemented for http client");
    }

    @Override
    public List<FileInfo> ls(@NotNull String remotePath, long limit, long offset) throws IOException, NotFoundException {
        throw new NotImplementedException("Not implemented for http client");
    }

    @Override
    public void insert(@NotNull String remotePath, @NotNull InputStream fileStream) throws IOException {
        throw new NotImplementedException("Not implemented for http client");
    }

    @Override
    public void mkdir(@NotNull String remotePath) throws IOException, NotFoundException {
        throw new NotImplementedException("Not implemented for http client");
    }

    @Override
    public void move(@NotNull String oldPath, @NotNull String newPath) throws IOException, NotFoundException {
        throw new NotImplementedException("Not implemented for http client");
    }

    @Override
    public void copy(@NotNull String currentPath, @NotNull String newPath) throws IOException, NotFoundException {
        throw new NotImplementedException("Not implemented for http client");
    }

    @Override
    public void delete(@NotNull String path) throws IOException {
        throw new NotImplementedException("Not implemented for http client");
    }

    @Override
    public InputStream getStream(@NotNull String path) throws IOException {
        return null;
    }

    @Override
    public InputStream getBytesByRange(@NotNull String path, long startByte, long count) throws IOException {
        throw new NotImplementedException("Not implemented for http client");
    }

    @Override
    public void putBytesByRange(String path, InputStream byteStream, long startByte, long endByte) throws IOException {
        throw new NotImplementedException("Not implemented for http client");
    }

    @Override
    public void append(@NotNull String path, @NotNull InputStream byteStream) throws IOException {
        throw new NotImplementedException("Not implemented for http client");
    }

    @Override
    public void download(String path) throws IOException {
        throw new NotImplementedException("Not implemented for http client");
    }

    @Override
    public void connect() throws IOException {
        throw new NotImplementedException("Not implemented for http client");
    }

    @Override
    public void disconnect() {
        throw new NotImplementedException("Not implemented for http client");
    }
}
