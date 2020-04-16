package edu.utexas.tacc.tapis.files.lib.clients;

import edu.utexas.tacc.tapis.files.lib.models.FileInfo;
import edu.utexas.tacc.tapis.systems.client.gen.model.TSystem;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.NotImplementedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.services.s3.paginators.ListObjectsV2Iterable;

import javax.validation.constraints.NotNull;
import javax.ws.rs.NotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

public class S3DataClient implements IRemoteDataClient {

    private final Logger log = LoggerFactory.getLogger(S3DataClient.class);
    private final S3Client client;
    private final String bucket;
    private final TSystem system;
    private final String rootDir;

    public S3DataClient(@NotNull TSystem remoteSystem) throws IOException {
        system = remoteSystem;
        bucket = system.getBucketName();
        rootDir = system.getRootDir();

        try {
            URI endpoint = new URI(system.getHost() + ":" + system.getPort());
            AwsCredentials credentials = AwsBasicCredentials.create(system.getAccessCredential().getAccessKey(), system.getAccessCredential().getAccessSecret());
            client = S3Client.builder()
                    .region(Region.AP_NORTHEAST_1)
                    .endpointOverride(endpoint)
                    .credentialsProvider(StaticCredentialsProvider.create(credentials))
                    .build();

        } catch (URISyntaxException e) {
            throw new IOException("Could not create s3 client for system");
        }
    }

    private Stream<S3Object> listWithIterator(String path) {
        ListObjectsV2Request req = ListObjectsV2Request.builder()
                .bucket(bucket)
                .prefix(path)
                .build();
        ListObjectsV2Iterable resp = client.listObjectsV2Paginator(req);
        return resp.contents().stream();
    }

    private FileInfo listSingleObject(String path) throws S3Exception, NotFoundException {
        try {
            HeadObjectRequest req = HeadObjectRequest.builder()
                    .bucket(bucket)
                    .key(path)
                    .build();
            HeadObjectResponse resp = client.headObject(req);
            FileInfo f = new FileInfo();
            f.setPath(path);
            f.setLastModified(resp.lastModified());
            f.setName(path);
            f.setSize(resp.contentLength());
            return f;
        } catch (NoSuchKeyException ex) {
            String msg = String.format("No such file at %s", path);
            throw new NotFoundException(msg);
        }
    }


    @Override
    public List<FileInfo> ls(@NotNull String path) throws IOException, NotFoundException {
        //TODO: Implement limit/offset
        String remoteAbsolutePath = DataClientUtils.getRemotePathForS3(rootDir, path);

//        // If it looks like a file, just try to get the HEAD info
//        if (!remoteAbsolutePath.endsWith("/")) {
//           FileInfo info = listSingleObject(remoteAbsolutePath);
//           List<FileInfo> out = new ArrayList<>();
//           out.add(info);
//           return out;
//        }
        Stream<S3Object> response = listWithIterator(remoteAbsolutePath);
        List<FileInfo> files = new ArrayList<>();
        response.forEach(x->{
                    files.add(new FileInfo(x));
                });
        if (files.size() == 0) {
            throw new NotFoundException("No file at path " + path);
        }
        return  files;
    }


    @Override
    public void mkdir(@NotNull String path) throws IOException {
        //TODO: Add sanitization for paths for things like ~, ../../.. yada yada

        String remotePath = DataClientUtils.getRemotePathForS3(rootDir, path);
        remotePath = DataClientUtils.ensureTrailingSlash(remotePath);
        PutObjectRequest req = PutObjectRequest.builder()
                .bucket(bucket)
                .key(remotePath)
                .build();
        client.putObject(req, RequestBody.fromString(""));
    }

    @Override
    public void insert(@NotNull String path, @NotNull InputStream fileStream) throws IOException {
        // TODO: This should use multipart on an InputStream ideally;
        String remotePath = DataClientUtils.getRemotePathForS3(rootDir, path);
        try {
            PutObjectRequest req = PutObjectRequest.builder()
                    .bucket(bucket)
                    .key(remotePath)
                    .build();
            client.putObject(req, RequestBody.fromBytes(fileStream.readAllBytes()));
        } catch (S3Exception ex) {
            log.error("S3DataClient::insert", ex);
            throw new IOException("Could not upload file.");
        }
    }


    private void doRename(S3Object object, String newPath) throws IOException{
        //Copy object to new path
        copy(object.key(), newPath);
        //Delete old object
        delete(object.key());
    }

    /**
     *  @param currentPath
     * @param newName
     */
    @Override
    public void move(@NotNull String currentPath, @NotNull String newName) {

        String oldRemotePath;
        oldRemotePath = FilenameUtils.normalizeNoEndSeparator(DataClientUtils.getRemotePath(rootDir, currentPath));
        Path tmp = Paths.get(oldRemotePath);
        String newRoot = FilenameUtils.normalizeNoEndSeparator(FilenameUtils.concat(tmp.getParent().toString(), newName));

        Stream<S3Object> response = listWithIterator(oldRemotePath);

        //TODO: retry logic?
        response.forEach(object -> {
            try {
                String key = object.key();
                String newKey = key.replaceFirst(oldRemotePath, newRoot);
                doRename(object, newKey);
            } catch (IOException ex) {
                log.error("S3DataClient::rename " + object.key(), ex);
            }
        });
    }

    @Override
    public void copy(@NotNull String currentPath, @NotNull String newPath) throws IOException {
        String encodedSourcePath = bucket + "/" + DataClientUtils.getRemotePath(rootDir, currentPath);
        String remoteDestinationPath = DataClientUtils.getRemotePathForS3(rootDir, newPath);
        CopyObjectRequest req = CopyObjectRequest.builder()
                .bucket(bucket)
                .copySource(encodedSourcePath)
                .key(remoteDestinationPath)
                .build();
        try {
            client.copyObject(req);
        } catch (NoSuchKeyException ex) {
            throw new NotFoundException();
        } catch (S3Exception ex) {
            log.error("S3DataClient::copy " + encodedSourcePath, ex);
            throw new IOException("Copy object failed at path " + encodedSourcePath);
        }

    }

    private  void deleteObject(String remotePath) throws S3Exception {
        DeleteObjectRequest req = DeleteObjectRequest.builder()
                .bucket(bucket)
                .key(remotePath)
                .build();
        client.deleteObject(req);
    }

    @Override
    public void delete(@NotNull String path) throws IOException, NotFoundException {
        try {
            String remotePath = DataClientUtils.getRemotePath(rootDir, path);
            listWithIterator(remotePath).forEach(object -> {
                //TODO: What to do if one fails?
                try {
                    deleteObject(object.key());
                } catch (S3Exception ex) {

                }
            });
        } catch (NoSuchKeyException ex) {
            throw new NotFoundException();
        } catch (S3Exception ex) {
            throw new IOException("Could not delete object");
        }
    }


    /**
     * Returns the entire contents of an object as an InputStream
     * @param path
     * @return
     * @throws IOException
     * @throws NotFoundException
     */
    @Override
    public InputStream getStream(@NotNull String path) throws IOException, NotFoundException {
        try {
            GetObjectRequest req = GetObjectRequest.builder()
                    .bucket(bucket)
                    .key(path)
                    .build();
            return client.getObject(req, ResponseTransformer.toInputStream());
        } catch (NoSuchKeyException ex) {
            throw new NotFoundException();
        } catch (S3Exception ex) {
            log.error(ex.getMessage());
            throw new IOException();
        }
    }

    @Override
    public void download(String path) throws IOException {

    }

    @Override
    public void connect() throws IOException{

    }

    @Override
    public void disconnect() {

    }

    @Override
    public InputStream getBytesByRange(@NotNull String path, long startByte, long count) throws IOException{
        try {
            // S3 api includes the final byte, different than posix, so we subtract one to get the proper count.
            String brange = String.format("bytes=%s-%s", startByte, startByte + count - 1);
            GetObjectRequest req = GetObjectRequest.builder()
                    .bucket(bucket)
                    .range(brange)
                    .key(path)
                    .build();
            return client.getObject(req, ResponseTransformer.toInputStream());
        } catch (NoSuchKeyException ex) {
            throw new NotFoundException();
        } catch (S3Exception ex) {
            log.error(ex.getMessage());
            throw new IOException();
        }
    }

    @Override
    public void putBytesByRange(String path, InputStream byteStream, long startByte, long endByte) throws IOException {
        throw new NotImplementedException("S3 does not support put by range operations");
    }

    @Override
    public void append(@NotNull String path, @NotNull InputStream byteStream) throws IOException {
        throw new NotImplementedException("S3 does not support append operations");
    }

}
