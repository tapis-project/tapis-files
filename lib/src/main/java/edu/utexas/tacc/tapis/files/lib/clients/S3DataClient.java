package edu.utexas.tacc.tapis.files.lib.clients;

import edu.utexas.tacc.tapis.files.lib.models.FileInfo;
import edu.utexas.tacc.tapis.systems.client.gen.model.TSystem;
import org.apache.commons.io.FilenameUtils;
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

    private Logger log = LoggerFactory.getLogger(S3DataClient.class);
    private S3Client client;
    private String bucket;
    private TSystem system;
    private String rootDir;

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


    @Override
    public List<FileInfo> ls(String path) throws IOException {
        //TODO: Implement limit/offset
        String remoteAbsolutePath = DataClientUtils.getRemotePathForS3(rootDir, path);
        Stream<S3Object> response = listWithIterator(remoteAbsolutePath);
        List<FileInfo> files = new ArrayList<>();
        response.forEach(x->{
                    files.add(new FileInfo(x));
                });
        return  files;
    }



    @Override
    public void mkdir(String path) throws IOException {
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
    public void insert(String path, InputStream fileStream) throws IOException {
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
     *
     * @param currentPath
     * @param newName
     */
    @Override
    public void move(String currentPath, String newName) {

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
    public void copy(String currentPath, String newPath) throws IOException {
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
    public void delete(String path) throws IOException {
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

    @Override
    public InputStream getStream(String path) throws IOException, NotFoundException {
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
    public void download(String path) {

    }

    @Override
    public void connect() throws IOException{

    }

    @Override
    public void disconnect() {

    }

}
