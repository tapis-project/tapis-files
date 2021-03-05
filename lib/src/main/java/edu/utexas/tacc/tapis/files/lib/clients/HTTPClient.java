package edu.utexas.tacc.tapis.files.lib.clients;

import edu.utexas.tacc.tapis.files.lib.models.FileInfo;

import org.jetbrains.annotations.NotNull;
import javax.ws.rs.NotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import org.apache.commons.lang3.NotImplementedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class HTTPClient implements IRemoteDataClient {

    private static final Logger log = LoggerFactory.getLogger(HTTPClient.class);

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
        OkHttpClient client = new OkHttpClient.Builder().build();
        Request request = new Request.Builder()
            .url(path)
            .build();

        Response response = client.newCall(request).execute();
        if (!response.isSuccessful()) {
            log.error("Could not retrieve file at path {}", path);
            throw new IOException("Unexpected code " + response);
        }
        return response.body().byteStream();
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
