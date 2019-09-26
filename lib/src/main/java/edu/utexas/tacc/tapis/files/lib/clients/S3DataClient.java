package edu.utexas.tacc.tapis.files.lib.clients;

import edu.utexas.tacc.tapis.files.lib.models.FileInfo;
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

    public S3DataClient() throws IOException {
        AwsCredentials credentials = AwsBasicCredentials.create("user", "password");

        try {
            client = S3Client.builder()
                    .region(Region.AP_NORTHEAST_1)
                    .endpointOverride(new URI("http://localhost:9000"))
                    .credentialsProvider(StaticCredentialsProvider.create(credentials))
                    .build();
        } catch (URISyntaxException e) {
            throw new IOException("Could not create s3 client for system");
        }
    }

    @Override
    public List<FileInfo> ls(String path) throws IOException {
        ListObjectsRequest req = ListObjectsRequest.builder()
                .bucket("test")
                .prefix("/")
                .build();
        ListObjectsResponse resp =this.client.listObjects(req);
        List files = new ArrayList();
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
