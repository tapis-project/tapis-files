package edu.utexas.tacc.tapis.files.lib.clients;

import edu.utexas.tacc.tapis.files.lib.models.FileInfo;
import edu.utexas.tacc.tapis.systems.client.gen.model.TapisSystem;
import org.jetbrains.annotations.NotNull;

import javax.ws.rs.NotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;

public class IrodsDataClient implements IRemoteDataClient {

    private String oboTenantId;
    private String oboUsername;
    private TapisSystem system;

    public IrodsDataClient(@NotNull String oboTenantId, @NotNull String oboUsername, @NotNull TapisSystem system) {
        this.oboTenantId = oboTenantId;
        this.oboUsername = oboUsername;
        this.system = system;
    }

    @Override
    public String getOboTenant() {
        return null;
    }

    @Override
    public String getOboUser() {
        return null;
    }

    @Override
    public String getSystemId() {
        return null;
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
}
