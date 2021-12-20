package edu.utexas.tacc.tapis.files.api.resources;

import io.jsonwebtoken.Jwt;
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.SignatureAlgorithm;
import io.jsonwebtoken.security.Keys;
import org.flywaydb.core.Flyway;
import org.glassfish.jersey.test.JerseyTestNg;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.BucketAlreadyOwnedByYouException;
import software.amazon.awssdk.services.s3.model.CreateBucketConfiguration;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;

import java.net.URI;
import java.security.Key;
import java.security.KeyPair;
import java.util.HashMap;
import java.util.Map;


@Test(groups={"integration"})
public abstract class BaseDatabaseIntegrationTest extends JerseyTestNg.ContainerPerClassTest
{
  // SSHConnection cache settings
  public static final long CACHE_MAX_SIZE = 20;
  public static final long CACHE_TIMEOUT_MINUTES = 1;

    public String getJwtForUser(String tenantId, String username) {
        Map<String, Object> claims = new HashMap<>();
        claims.put("tapis/tenant_id", tenantId);
        claims.put("tapis/token_type", "access");
        claims.put("tapis/delegation", false);
        claims.put("tapis/delegation_sub", null);
        claims.put("tapis/username", username);
        claims.put("tapis/account_type", "user");

        KeyPair keyPair = Keys.keyPairFor(SignatureAlgorithm.RS256);
        String jwt = Jwts.builder()
            .setSubject(username + "@" + tenantId)
            .setClaims(claims)
            .signWith(keyPair.getPrivate()).compact();
        return jwt;
    }

    public String getServiceJwt() {
        Map<String, Object> claims = new HashMap<>();
        claims.put("tapis/tenant_id", "dev");
        claims.put("tapis/token_type", "access");
        claims.put("tapis/delegation", false);
        claims.put("tapis/delegation_sub", null);
        claims.put("tapis/username", "service1");
        claims.put("tapis/account_type", "service");
        claims.put("tapis/target_site", "tacc");

        KeyPair keyPair = Keys.keyPairFor(SignatureAlgorithm.RS256);
        String serviceJwt = Jwts.builder()
            .setSubject("jobs@dev")
            .setClaims(claims)
            .signWith(keyPair.getPrivate()).compact();
        return serviceJwt;
    }

    @BeforeTest
    public void doFlywayMigrations() {
        Flyway flyway = Flyway.configure()
            .dataSource("jdbc:postgresql://localhost:5432/test", "test", "test")
            .load();
        flyway.clean();
        flyway.migrate();
    }


    @BeforeTest
    public void createTestBucket() {
        Region region = Region.US_WEST_2;
        String bucket = "test";
        AwsCredentials credentials = AwsBasicCredentials.create(
            "user",
            "password"
        );
        S3Client s3 = S3Client.builder()
            .region(region)
            .credentialsProvider(StaticCredentialsProvider.create(credentials))
            .endpointOverride(URI.create("http://localhost:9000"))
            .build();

        CreateBucketRequest createBucketRequest = CreateBucketRequest
            .builder()
            .bucket(bucket)
            .createBucketConfiguration(CreateBucketConfiguration.builder()
                .locationConstraint(region.id())
                .build())
            .build();

        try {
            s3.createBucket(createBucketRequest);
        } catch (BucketAlreadyOwnedByYouException ex) {

        }
    }


}
