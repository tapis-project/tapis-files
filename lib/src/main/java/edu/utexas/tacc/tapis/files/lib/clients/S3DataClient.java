package edu.utexas.tacc.tapis.files.lib.clients;

import edu.utexas.tacc.tapis.files.lib.models.FileInfo;
import edu.utexas.tacc.tapis.files.lib.utils.Constants;
import edu.utexas.tacc.tapis.files.lib.utils.PathUtils;
import edu.utexas.tacc.tapis.files.lib.utils.S3URLParser;
import edu.utexas.tacc.tapis.systems.client.gen.model.TSystem;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.commons.lang3.StringUtils;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;
import software.amazon.awssdk.services.s3.model.CopyObjectRequest;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.model.S3Object;
import software.amazon.awssdk.services.s3.paginators.ListObjectsV2Iterable;

import javax.ws.rs.NotFoundException;
import javax.ws.rs.core.UriBuilder;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.stream.Stream;

public class S3DataClient implements IRemoteDataClient {

    private final Logger log = LoggerFactory.getLogger(S3DataClient.class);

    public S3Client getClient() {
        return client;
    }

    private final S3Client client;
    private final String bucket;
    private final TSystem system;
    private final String rootDir;
    private static final int MAX_LISTING_SIZE = Constants.MAX_LISTING_SIZE;

    public S3DataClient(@NotNull TSystem remoteSystem) throws IOException {
        system = remoteSystem;
        bucket = system.getBucketName();
        rootDir = system.getRootDir();

        // There are so many different flavors of s3 URLs we have to
        // do the gymnastics below.
        try {
            String host = system.getHost();
            String region = S3URLParser.getRegion(host);
            URI endpoint = configEndpoint(host);
            Region reg;

            //For minio/other S3 compliant APIs, the region is not needed
            if (region == null) {
                reg = Region.US_EAST_1;
            } else {
                reg = Region.of(region);
            }
            AwsCredentials credentials = AwsBasicCredentials.create(
                system.getAuthnCredential().getAccessKey(),
                system.getAuthnCredential().getAccessSecret()
            );
            S3ClientBuilder builder = S3Client.builder()
                .region(reg)
                .credentialsProvider(StaticCredentialsProvider.create(credentials));

            // Have to do the endpoint override if its not a real AWS route, as in for a minio
            // instance
            if (!S3URLParser.isAWSUrl(host)) {
                builder.endpointOverride(endpoint);
            }
            client = builder.build();

        } catch (URISyntaxException e) {
            throw new IOException("Could not create s3 client for system");
        }
    }


    public URI configEndpoint(String host) throws URISyntaxException {
        URI endpoint;
        URI tmpURI = new URI(host);
        UriBuilder uriBuilder = UriBuilder.fromUri("");
        uriBuilder
            .host(tmpURI.getHost())
            .scheme(tmpURI.getScheme());
        if ((system.getPort() != null) && (system.getPort() > 0)) {
            uriBuilder.port(system.getPort());
        }
        if (StringUtils.isBlank(tmpURI.getHost())) {
            uriBuilder.host(host);
        }
        //Make sure there is a scheme, and default to https if not.
        if (StringUtils.isBlank(tmpURI.getScheme())) {
            uriBuilder.scheme("https");
        }
        endpoint = uriBuilder.build();
        return endpoint;
    }

    private Stream<S3Object> listWithIterator(String path) {
        ListObjectsV2Request req = ListObjectsV2Request.builder()
            .bucket(bucket)
            .prefix(path)
            .maxKeys(MAX_LISTING_SIZE)
            .build();
        ListObjectsV2Iterable resp = client.listObjectsV2Paginator(req);
        return resp.contents().stream();
    }

    private void doesExist(String path) throws NotFoundException {
        String remoteAbsolutePath = DataClientUtils.getRemotePathForS3(rootDir, path);
        try {
            HeadObjectRequest req = HeadObjectRequest.builder()
                .bucket(bucket)
                .key(remoteAbsolutePath)
                .build();
            client.headObject(req);
        } catch (NoSuchKeyException ex) {
            String msg = String.format("No such file at %s", path);
            throw new NotFoundException(msg);
        }
    }


    public List<FileInfo> ls(@NotNull String path) throws IOException, NotFoundException {
        return this.ls(path, MAX_LISTING_SIZE, 0);
    }

    @Override
    public void makeBucket(String name) {
        CreateBucketRequest req = CreateBucketRequest.builder()
            .bucket(name)
            .build();
        client.createBucket(req);
    }

    @Override
    public List<FileInfo> ls(@NotNull String path, long limit, long offset) throws IOException, NotFoundException {

        String remoteAbsolutePath = DataClientUtils.getRemotePathForS3(rootDir, path);

        Stream<S3Object> response = listWithIterator(remoteAbsolutePath);
        List<FileInfo> files = new ArrayList<>();
        response.skip(offset).limit(limit).forEach((S3Object x) -> {
            files.add(new FileInfo(x));
        });
        if (files.isEmpty()) {
            doesExist(remoteAbsolutePath);
        }
        return files;
    }


    @Override
    public void mkdir(@NotNull String path) throws IOException, NotFoundException {
        String remotePath = DataClientUtils.getRemotePathForS3(rootDir, path);
        remotePath = DataClientUtils.ensureTrailingSlash(remotePath);
        try {
            PutObjectRequest req = PutObjectRequest.builder()
                .bucket(bucket)
                .key(remotePath)
                .build();
            client.putObject(req, RequestBody.fromString(""));
        } catch (S3Exception ex) {
            log.error("S3DataClient.mkdir", ex);
            throw new IOException("Could not create directory.");
        }
    }

    @Override
    public void insert(@NotNull String path, @NotNull InputStream fileStream) throws IOException {
        // TODO: This should use multipart on an InputStream ideally;
        String remotePath = DataClientUtils.getRemotePathForS3(rootDir, path);
        File scratchFile = File.createTempFile(UUID.randomUUID().toString(), "tmp");
        try {
            FileUtils.copyInputStreamToFile(fileStream, scratchFile);
            PutObjectRequest req = PutObjectRequest.builder()
                .bucket(bucket)
                .key(remotePath)
                .build();
            client.putObject(req, RequestBody.fromFile(scratchFile));
        } catch (S3Exception ex) {
            log.error("S3DataClient::insert", ex);
            throw new IOException("Could not upload file.");
        } finally {
            scratchFile.delete();
        }
    }


    private void renameObject(S3Object object, String newPath, boolean withDelete) throws IOException {
        //Copy object to new path
        doCopy(object.key(), newPath);
        //Delete old object
        if (withDelete) {
            delete(object.key());
        }
    }


    /**
     * @param currentPath
     * @param newPath
     */
    @Override
    public void move(@NotNull String currentPath, @NotNull String newPath) throws IOException, NotFoundException {
        String remotePath = DataClientUtils.getRemotePathForS3(rootDir, currentPath);

        Stream<S3Object> response = listWithIterator(remotePath);
        response.forEach(object -> {
            try {
                String key = object.key();
                Path renamedPath = PathUtils.relativizePaths(remotePath, key, newPath);
                String newKey = renamedPath.normalize().toString();
                renameObject(object, newKey, true);
            } catch (IOException ex) {
                log.error("S3DataClient::move " + object.key(), ex);
            }
        });
    }

    private void doCopy(@NotNull String currentPath, @NotNull String newPath) throws NotFoundException, IOException {
        String encodedSourcePath = bucket + "/" + DataClientUtils.getRemotePath(rootDir, currentPath);
        String remoteDestinationPath = DataClientUtils.getRemotePathForS3(rootDir, newPath);
        CopyObjectRequest req = CopyObjectRequest.builder()
            .destinationBucket(bucket)
            .copySource(encodedSourcePath)
            .destinationKey(remoteDestinationPath)
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

    @Override
    public void copy(@NotNull String currentPath, @NotNull String newPath) throws IOException, NotFoundException {
        String remotePath = DataClientUtils.getRemotePathForS3(rootDir, currentPath);
        Stream<S3Object> response = listWithIterator(remotePath);
        response.forEach(object -> {
            try {
                String key = object.key();
                Path renamedPath = PathUtils.relativizePaths(currentPath, key, newPath);
                String newKey = renamedPath.normalize().toString();
                renameObject(object, newKey, false);
            } catch (IOException ex) {
                log.error("S3DataClient::move " + object.key(), ex);
            }
        });
    }

    private void deleteObject(String remotePath) throws S3Exception {
        DeleteObjectRequest req = DeleteObjectRequest.builder()
            .bucket(bucket)
            .key(remotePath)
            .build();
        client.deleteObject(req);
    }

    @Override
    public void delete(@NotNull String path) throws IOException, NotFoundException {
        try {
            String remotePath = DataClientUtils.getRemotePathForS3(rootDir, path);
            listWithIterator(remotePath).forEach(object -> {
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
     *
     * @param path
     * @return
     * @throws IOException
     * @throws NotFoundException
     */
    @Override
    public InputStream getStream(@NotNull String path) throws IOException, NotFoundException {
        String remotePath = DataClientUtils.getRemotePathForS3(rootDir, path);
        try {
            GetObjectRequest req = GetObjectRequest.builder()
                .bucket(bucket)
                .key(remotePath)
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
    public void connect() throws IOException {

    }

    @Override
    public void disconnect() {

    }

    @Override
    public InputStream getBytesByRange(@NotNull String path, long startByte, long count) throws IOException, NotFoundException {
        String remotePath = DataClientUtils.getRemotePathForS3(rootDir, path);
        try {
            // S3 api includes the final byte, different than posix, so we subtract one to get the proper count.
            String brange = String.format("bytes=%s-%s", startByte, startByte + count - 1);
            GetObjectRequest req = GetObjectRequest.builder()
                .bucket(bucket)
                .range(brange)
                .key(remotePath)
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
