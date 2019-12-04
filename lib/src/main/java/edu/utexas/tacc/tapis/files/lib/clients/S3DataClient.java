package edu.utexas.tacc.tapis.files.lib.clients;

import edu.utexas.tacc.tapis.files.lib.models.FileInfo;
import edu.utexas.tacc.tapis.systems.client.gen.model.TSystem;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.ListObjectsRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsResponse;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;

public class S3DataClient implements IRemoteDataClient {

    private S3Client client;
    private String bucket;

    //public S3DataClient(FakeSystem system) throws IOException {
    public S3DataClient(TSystem system) throws IOException {
        this.bucket = system.getBucketName();
        try {
            URI endpoint = new URI(system.getHost() + ":" + system.getPort());
            //AwsCredentials credentials = AwsBasicCredentials.create(system.getUsername(), system.getPassword());
            AwsCredentials credentials = AwsBasicCredentials.create(system.getEffectiveUserId(), system.getAccessCredential());
            client = S3Client.builder()
                    .region(Region.AP_NORTHEAST_1)
                    .endpointOverride(endpoint)
                    .credentialsProvider(StaticCredentialsProvider.create(credentials))
                    .build();
        } catch (URISyntaxException e) {
            throw new IOException("Could not create s3 client for system");
        }
    }

    @Override
    public List<FileInfo> ls(String path) throws IOException {
        ListObjectsRequest req = ListObjectsRequest.builder()
                .bucket(bucket)
                .prefix(path)
                .build();
        ListObjectsResponse resp = client.listObjects(req);
        List<FileInfo> files = new ArrayList<>();
        resp.contents().stream().forEach(x->{
            files.add(new FileInfo(x));
        });
        return  files;
    }

    @Override
    public void move(String srcSystem, String srcPath, String destSystem, String destPath) {

    }

    @Override
    public void rename() {

    }

    @Override
    public void copy() {

    }

    @Override
    public void delete() {

    }

    @Override
    public void getStream() {

    }

    @Override
    public void download() {

    }

    @Override
    public void connect() {

    }

    @Override
    public void disconnect() {

    }

}
