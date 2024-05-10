package edu.utexas.tacc.tapis.files.integration.transfers;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import edu.utexas.tacc.tapis.files.lib.models.FileInfo;
import edu.utexas.tacc.tapis.files.lib.models.TransferTaskStatus;
import edu.utexas.tacc.tapis.files.test.RandomByteInputStream;
import edu.utexas.tacc.tapis.files.test.TestUtils;
import edu.utexas.tacc.tapis.shared.ssh.SshSessionPool;
import edu.utexas.tacc.tapis.shared.utils.TapisGsonUtils;
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.SignatureAlgorithm;
import io.jsonwebtoken.security.Keys;
import org.apache.commons.lang3.StringUtils;
import org.glassfish.grizzly.http.Method;
import org.glassfish.jersey.client.ClientProperties;
import org.glassfish.jersey.media.multipart.FormDataMultiPart;
import org.glassfish.jersey.media.multipart.MultiPartFeature;
import org.glassfish.jersey.media.multipart.file.FileDataBodyPart;
import org.glassfish.jersey.media.multipart.file.StreamDataBodyPart;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.Invocation;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.lang.reflect.Field;
import java.nio.file.Path;
import java.security.DigestInputStream;
import java.security.KeyPair;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class IntegrationTestUtils {
    private Logger log = LoggerFactory.getLogger(IntegrationTestUtils.class);
    private static final int CHUNK_MAX = 1000;
    private static final String TAPIS_TOKEN_HEADER = "X-Tapis-Token";
    public static final IntegrationTestUtils instance = new IntegrationTestUtils();
    public final Set<TransferTaskStatus> terminalStates;
    private String SHA256 = "SHA-256";

    public static class TransferDefinition {
        private String sourcePath;
        private String destinationPath;

        public String getSourcePath() {
            return sourcePath;
        }

        public void setSourcePath(String sourcePath) {
            this.sourcePath = sourcePath;
        }

        public String getDestinationPath() {
            return destinationPath;
        }

        public void setDestinationPath(String destinationPath) {
            this.destinationPath = destinationPath;
        }
    }

    private class RandomStreamWriter implements Runnable {
        boolean completedSuccessfully = false;
        String digest = null;

        private final int bytesToWrite;
        private final boolean alhapNumericOnly;
        private PipedOutputStream outputStream;

        public RandomStreamWriter(int bytesToWrite, boolean alhapNumericOnly) {
            this.bytesToWrite = bytesToWrite;
            this.alhapNumericOnly = alhapNumericOnly;
        }

        public InputStream initInputStream() throws IOException {
            outputStream = new PipedOutputStream();
            return new PipedInputStream(outputStream);
        }

        @Override
        public void run() {
            try {
                RandomByteInputStream randomInputStream = new RandomByteInputStream(bytesToWrite,
                        RandomByteInputStream.SizeUnit.BYTES, alhapNumericOnly);
                DigestInputStream digestInputStream = new DigestInputStream(randomInputStream, MessageDigest.getInstance(SHA256));
                digestInputStream.transferTo(outputStream);
                digest = TestUtils.hashAsHex(digestInputStream.getMessageDigest().digest());
                completedSuccessfully = true;
            } catch (Throwable th) {
                throw new RuntimeException(th);
            } finally {
                try {
                    outputStream.close();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        }

        public String getDigest() {
            return digest;
        }

        public boolean isCompletedSuccessfully() {
            return completedSuccessfully;
        }
    };


    IntegrationTestUtils() {
        terminalStates = new HashSet<>();
        terminalStates.add(TransferTaskStatus.COMPLETED);
        terminalStates.add(TransferTaskStatus.FAILED);
        terminalStates.add(TransferTaskStatus.CANCELLED);
        terminalStates.add(TransferTaskStatus.PAUSED);
    }

    public void mkdir(String baseUrl, String token, String systemId, Path destinationPath) {
        Client client = ClientBuilder.newClient().register(MultiPartFeature.class);
        WebTarget target = client.target(baseUrl);
        Invocation.Builder invocationBuilder = target.path(getUrlPath("ops", systemId, Path.of(""))).request(MediaType.APPLICATION_JSON);
        JsonObject jsonObject = new JsonObject();
        jsonObject.addProperty("path", destinationPath.toString());
        Response response = invocationBuilder.header(TAPIS_TOKEN_HEADER, token)
                .post(Entity.json(jsonObject.toString()));
        Assert.assertEquals(response.getStatus(), 200);
    }

    public void uploadFile(String baseUrl, String token, String systemId, File sourceFile, Path destinationPath) {
        Client client = ClientBuilder.newClient().register(MultiPartFeature.class);
        WebTarget target = client.target(baseUrl);
        Invocation.Builder invocationBuilder = target.path(getUrlPath("ops", systemId, destinationPath)).request(MediaType.APPLICATION_JSON);

        FileDataBodyPart filePart = new FileDataBodyPart("file", sourceFile);
        FormDataMultiPart formDataMultiPart = (FormDataMultiPart) new FormDataMultiPart().bodyPart(filePart);

        Response response = invocationBuilder.header(TAPIS_TOKEN_HEADER, token)
                .post(Entity.entity(formDataMultiPart, formDataMultiPart.getMediaType()));
        Assert.assertEquals(response.getStatus(), 200);
    }

    public String uploadRandomFile(String baseUrl, String token, String systemId, Path destinationPath,
                           int size, boolean alphaNumericOnly) {
        Client client = ClientBuilder.newClient().register(MultiPartFeature.class);
        WebTarget target = client.target(baseUrl);
        Invocation.Builder invocationBuilder = target.path(getUrlPath("ops", systemId, destinationPath)).request(MediaType.APPLICATION_JSON);

        RandomStreamWriter writer = new RandomStreamWriter(size, alphaNumericOnly);
        try {
            StreamDataBodyPart filePart = new StreamDataBodyPart("file", writer.initInputStream(), destinationPath.getFileName().toString());

            Thread fileWriterThread = new Thread(writer);
            fileWriterThread.start();
            FormDataMultiPart formDataMultiPart = (FormDataMultiPart) new FormDataMultiPart().bodyPart(filePart);

            Response response = invocationBuilder.header(TAPIS_TOKEN_HEADER, token)
                    .post(Entity.entity(formDataMultiPart, formDataMultiPart.getMediaType()));

            fileWriterThread.join();
            Assert.assertTrue(writer.isCompletedSuccessfully());
            Assert.assertEquals(response.getStatus(), 200);
        } catch (Exception ex) {
            Assert.fail("Exception during writing upload files", ex);
        }

        return writer.getDigest();
    }

    public JsonObject transferFiles(String baseUrl, String token, String tag, TransferDefinition ... transferDefinitions) {
        JsonArray elements = new JsonArray();
        for(TransferDefinition definition : transferDefinitions) {
            JsonObject transfer = new JsonObject();
            transfer.addProperty("sourceURI", definition.sourcePath);
            transfer.addProperty("destinationURI", definition.destinationPath);
            elements.add(transfer);
        }
        JsonObject transferRequest = new JsonObject();
        transferRequest.addProperty("tag", tag);
        transferRequest.add("elements", elements);

        Response response = ClientBuilder.newClient().target(baseUrl)
                .path("transfers").request(MediaType.APPLICATION_JSON)
                .header(TAPIS_TOKEN_HEADER, token)
                .post(Entity.json(TapisGsonUtils.getGson().toJson(transferRequest)));
        Assert.assertEquals(response.getStatus(), 200);
        String responseString =  response.readEntity(String.class);
        JsonObject jsonObject = TapisGsonUtils.getGson().fromJson(responseString, JsonObject.class);
        return TapisGsonUtils.getGson().fromJson(jsonObject, JsonObject.class);
    }

    public void downloadAndVerify(String baseUrl, String token, String systemId, Path filePath, String expectedDigest)
            throws IOException, NoSuchAlgorithmException {
        Response response = ClientBuilder.newClient().target(baseUrl)
                .path(getUrlPath("content", systemId, filePath)).request(MediaType.APPLICATION_OCTET_STREAM)
                .header(TAPIS_TOKEN_HEADER, token)
                .get();
        Assert.assertEquals(response.getStatus(), 200);
        InputStream is = (InputStream) response.getEntity();
        MessageDigest digest = MessageDigest.getInstance(SHA256);
        long totalBytesRead = 0;
        int bytesRead = 0;
        byte[] chunk = new byte[CHUNK_MAX];
        do {
            bytesRead = is.read(chunk);
            if(bytesRead > 0) {
                totalBytesRead += bytesRead;
                digest.update(chunk, 0, bytesRead);
            }
        } while (bytesRead > 0);

        String hexDigest = TestUtils.hashAsHex(digest.digest());
        String contentLengthHeader = response.getHeaderString("content-length");
        if(StringUtils.isBlank(contentLengthHeader)) {
            log.warn("WARNING:  No content length for file.  System: " + systemId + "  BytesRead: " + totalBytesRead + "  Path: " + filePath + "  ExpectedDigest: " + expectedDigest + "  ActualDigest: " + hexDigest);
        } else {
            Assert.assertEquals(totalBytesRead, Long.parseLong(contentLengthHeader));
        }

        log.info("Checking file.  System: " + systemId + "  BytesRead: " + totalBytesRead + "  Path: " + filePath + "  ExpectedDigest: " + expectedDigest + "  ActualDigest: " + hexDigest);

        Assert.assertEquals(hexDigest, expectedDigest);
    }
    public Collection<JsonObject> waitForTransfers(String baseUrl, String token, List<String> transferTaskIds,
                                                   long maxWaitMillis) throws Exception {
        return waitForTransfers(baseUrl, token, transferTaskIds, maxWaitMillis, Executors.newSingleThreadExecutor());
    }

    public Collection<JsonObject> waitForTransfers(String baseUrl, String token, List<String> transferTaskIds,
                                                   long maxWaitMillis, ExecutorService threadPool) throws Exception {
        Map<String, JsonObject> completedTransfers = new HashMap<>();
        List<String> uncompletedTransfers = new ArrayList<>();
        uncompletedTransfers.addAll(transferTaskIds);
        long startTime = System.currentTimeMillis();

        while(!uncompletedTransfers.isEmpty()) {
            List<Future<JsonObject>> statusFutures = new ArrayList<>();
            for (String transferTaskId : uncompletedTransfers) {
                Future<JsonObject> statusFuture = threadPool.submit(new Callable<JsonObject>() {
                    @Override
                    public JsonObject call() throws Exception {
                        JsonObject result = checkTransfer(baseUrl, token, transferTaskId);
                        return result;
                    }
                });
                statusFutures.add(statusFuture);
            }

            for(Future<JsonObject> statusFuture : statusFutures) {
                JsonObject result = statusFuture.get();
                TransferTaskStatus transferTaskStatus = TransferTaskStatus.valueOf(result.get("result").getAsJsonObject().get("status").getAsString());
                String transferTaskId = result.get("result").getAsJsonObject().get("uuid").getAsString();
                if (terminalStates.contains(transferTaskStatus)) {
                    log.info("Transfer Done: " + transferTaskId + " Status: " + transferTaskStatus);
                    Assert.assertEquals(transferTaskStatus, TransferTaskStatus.COMPLETED);
                    completedTransfers.put(transferTaskId, result);
                } else {
                    log.info("Waiting for transfer: " + transferTaskId);
                }
            }

            uncompletedTransfers.removeAll(completedTransfers.keySet());
            long elapsedTime = System.currentTimeMillis() - startTime;
            if((uncompletedTransfers.size() > 0)) {
                if (elapsedTime > maxWaitMillis) {
                    Assert.fail("Timeout waiting for transfers to complete");
                } else {
                    // there's still uncompleted transfers, and we have mre time before
                    // the max wait time.  Sleep a second to allow some time for
                    // transfers before checking agian.
                    log.info(String.format(" --- Uncompleted transfers: %d ---", uncompletedTransfers.size()));
                    Thread.sleep(1000);
                }
            }
        }

        return completedTransfers.values();
    }

    public JsonObject checkTransfer(String baseUrl, String token, String transferTaskId) {
        Client client = ClientBuilder.newClient().register(MultiPartFeature.class);
        Response response = client.target(baseUrl)
                .path("transfers/" + transferTaskId).request(MediaType.APPLICATION_JSON)
                .header(TAPIS_TOKEN_HEADER, token)
                .get();
        Assert.assertEquals(response.getStatus(), 200);
        String jsonResponse = response.readEntity(String.class);
        return TapisGsonUtils.getGson().fromJson(jsonResponse, JsonObject.class);
    }

    public List<FileInfo> getListing(String baseUrl, String token, String systemId, Path path) {
        Client client = ClientBuilder.newClient().register(MultiPartFeature.class);
        Response response = client.target(baseUrl)
                .path(getUrlPath("ops", systemId, path)).request(MediaType.APPLICATION_JSON)
                .header(TAPIS_TOKEN_HEADER, token)
                .get();
        Assert.assertEquals(response.getStatus(), 200);
        String json = response.readEntity(String.class);
        JsonObject jsonResponse = TapisGsonUtils.getGson().fromJson(json, JsonObject.class);
        JsonArray resultArray = jsonResponse.getAsJsonArray("result");
        List<FileInfo> fileInfos = new ArrayList<>();
        for(int i=0 ; i < resultArray.size() ; i++) {
            fileInfos.add(TapisGsonUtils.getGson().fromJson(resultArray.get(i), FileInfo.class));
        }
        return fileInfos;
    }

    public void deletePath(String baseUrl, String token, String systemId, Path path) {
        if((path == null) || (path.equals(Path.of("/"))
                || StringUtils.isBlank(path.toString())
                || StringUtils.equals(path.toString(), "/"))) {
            throw new RuntimeException("DONT DELETE THE ROOT PATH");
        }

        Client client = ClientBuilder.newClient().register(MultiPartFeature.class);
        Response response = client.target(baseUrl)
                .path(getUrlPath("ops", systemId, path)).request(MediaType.APPLICATION_JSON)
                .header(TAPIS_TOKEN_HEADER, token)
                .delete();
        if(response.getStatus() != 404) {
            Assert.assertEquals(response.getStatus(), 200);
        } else {
            log.info("Path not found on system.  System:" + systemId + " Path: " + path);
        }
    }

    public String getToken(String tokenUrl, String username, String password) {
        JsonObject tokenRequest = new JsonObject();
        tokenRequest.addProperty("username", username);
        tokenRequest.addProperty("password", password);
        tokenRequest.addProperty("grant_type", "password");
        Response response = ClientBuilder.newClient().property(ClientProperties.SUPPRESS_HTTP_COMPLIANCE_VALIDATION, true)
                .target(tokenUrl).path("tokens").request(MediaType.APPLICATION_JSON)
                .method(Method.GET.toString(), Entity.json(tokenRequest.toString()));
        Assert.assertEquals(response.getStatus(), 200);
        String jsonString = response.readEntity(String.class);
        JsonObject responseObject = TapisGsonUtils.getGson().fromJson(jsonString, JsonObject.class);
        return responseObject.get("result").getAsJsonObject().get("access_token").getAsJsonObject().get("access_token").getAsString();
    }

    private String getUrlPath(String httpOperation, String systemId, Path path) {
        return Path.of(httpOperation, systemId, path.toString()).toString();
    }

    public static void clearSshSessionPoolInstance() {
        try {
            Field f = SshSessionPool.class.getDeclaredField("instance");
            f.setAccessible(true);
            f.set(null, null);
        } catch (NoSuchFieldException | IllegalAccessException ex) {
            System.out.println("Unable to clear ssh session pool instance.  Exception: " + ex);
        }
    }

    public static String getJwtForUser(String tenantId, String username) {
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

    public static String getServiceJwt() {
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
}
