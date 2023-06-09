package edu.utexas.tacc.tapis.files.integration.transfers;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import edu.utexas.tacc.tapis.files.lib.models.TransferTaskStatus;
import edu.utexas.tacc.tapis.shared.utils.TapisGsonUtils;
import org.apache.commons.lang3.RandomStringUtils;
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
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

public class TestUtils {
    private Logger log = LoggerFactory.getLogger(TestUtils.class);
    private static final int CHUNK_MAX = 1000;
    private static final String TAPIS_TOKEN_HEADER = "X-Tapis-Token";
    private static final String TEMP_FILE_PREFIX = "Digest:";
    private static final String TEMP_FILE_SUFFIX = ".tmp:";
    public static final TestUtils instance = new TestUtils();
    private final Set<TransferTaskStatus> terminalStates;

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

        private final long bytesToWrite;
        private final boolean alhapNumericOnly;
        private PipedOutputStream outputStream;

        public RandomStreamWriter(long bytesToWrite, boolean alhapNumericOnly) {
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
                digest = writeRandomBytes(outputStream, bytesToWrite, alhapNumericOnly);
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


    TestUtils() {
        terminalStates = new HashSet<>();
        terminalStates.add(TransferTaskStatus.COMPLETED);
        terminalStates.add(TransferTaskStatus.FAILED);
        terminalStates.add(TransferTaskStatus.CANCELLED);
        terminalStates.add(TransferTaskStatus.PAUSED);
    }

    public void uploadFile(String baseUrl, String token, String systemId, File sourceFile, Path destinationPath) {
        Client client = ClientBuilder.newClient().register(MultiPartFeature.class);
        WebTarget target = client.target(baseUrl);
        Invocation.Builder invocationBuilder = target.path("ops/" + systemId + "/" + destinationPath).request(MediaType.APPLICATION_JSON);

        FileDataBodyPart filePart = new FileDataBodyPart("file", sourceFile);
        FormDataMultiPart formDataMultiPart = (FormDataMultiPart) new FormDataMultiPart().bodyPart(filePart);

        Response response = invocationBuilder.header(TAPIS_TOKEN_HEADER, token)
                .post(Entity.entity(formDataMultiPart, formDataMultiPart.getMediaType()));
        Assert.assertEquals(response.getStatus(), 200);
    }

    public String uploadRandomFile(String baseUrl, String token, String systemId, Path destinationPath,
                           long size, boolean alphaNumericOnly) {
        Client client = ClientBuilder.newClient().register(MultiPartFeature.class);
        WebTarget target = client.target(baseUrl);
        Invocation.Builder invocationBuilder = target.path("ops/" + systemId + "/" + destinationPath).request(MediaType.APPLICATION_JSON);

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
        Path path = Path.of("content", systemId, filePath.toString());
        Response response = ClientBuilder.newClient().target(baseUrl)
                .path(path.toString()).request(MediaType.APPLICATION_OCTET_STREAM)
                .header(TAPIS_TOKEN_HEADER, token)
                .get();
        Assert.assertEquals(response.getStatus(), 200);
        int bytesLeft = Integer.parseInt(response.getHeaderString("content-length"));
        InputStream is = (InputStream) response.getEntity();
        MessageDigest digest = MessageDigest.getInstance("SHA-256");
        while(bytesLeft > 0) {
            int chunkSize = bytesLeft > CHUNK_MAX ? CHUNK_MAX : bytesLeft;
            byte [] chunk = new byte[chunkSize];
            bytesLeft -= is.read(chunk);
            digest.update(chunk);
        }
        String hexDigest = hashAsHex(digest.digest());
        Assert.assertEquals(hexDigest, expectedDigest);
    }

    public List<Path> createDigestedLocalFiles(Path directoryPath, int count, long size, boolean textOnly) throws IOException, NoSuchAlgorithmException {
        List<Path> createdFiles = new ArrayList<>();
        for(int i=0;i<count;i++) {
            Path filePath = Files.createTempFile(directoryPath, TEMP_FILE_PREFIX, TEMP_FILE_SUFFIX);
            OutputStream outStream = new FileOutputStream(filePath.toFile());
            Path digestFileName = Path.of(directoryPath.toString(), writeRandomBytes(outStream, size, textOnly));
            filePath.toFile().renameTo(digestFileName.toFile());
            createdFiles.add(filePath);
        }

        return createdFiles;
    }

    private String writeRandomBytes(OutputStream outStream, long size, boolean textOnly) throws IOException, NoSuchAlgorithmException {
        Random random = new Random(System.currentTimeMillis());
        MessageDigest digest = MessageDigest.getInstance("SHA-256");

        long bytesWritten = 0;
        while (bytesWritten < size) {
           long bytesLeft = size - bytesWritten;
            byte[] chunk = new byte[bytesLeft < CHUNK_MAX ? (int)bytesLeft : CHUNK_MAX];
            if(textOnly) {
                chunk = RandomStringUtils.randomAlphanumeric(chunk.length).getBytes();
            } else {
                random.nextBytes(chunk);
            }
            digest.update(chunk);
            outStream.write(chunk);
            bytesWritten += chunk.length;
        }
        String hexDigest = hashAsHex(digest.digest());
        return hexDigest;
    }


    public static String hashAsHex(byte[] hashBytes) {
        StringBuilder hexString = new StringBuilder(2 * hashBytes.length);
        for (int i = 0; i < hashBytes.length; i++) {
            String hex = Integer.toHexString(0xff & hashBytes[i]);
            if(hex.length() == 1) {
                hexString.append('0');
            }
            hexString.append(hex);
        }
        return "sha256:" + hexString.toString();
    }

    // TODO: cleanup - don't want to keep worring about deletes during refactors.
    public void cleanupDirs(String prefix, Path deleteDir, int max_depth) throws IOException {
        /*
        Files.walkFileTree(deleteDir, Collections.emptySet(), max_depth, new SimpleFileVisitor<>() {

            @Override
            public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) throws IOException {
                if((dir.getFileName().toString().startsWith(prefix)) || (deleteDir.equals(dir))) {
                    return FileVisitResult.CONTINUE;
                }
                return FileVisitResult.SKIP_SUBTREE;
            }

            @Override
            public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
                if(dir.getFileName().toString().startsWith(prefix)) {
                    Files.delete(dir);
                }
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                if(Files.isRegularFile(file)) {
                    Files.delete(file);
                }
                return FileVisitResult.CONTINUE;
            }
        });
         */
    }

    public Collection<JsonObject> waitForTransfers(String baseUrl, String token, List<String> transferTaskIds, long maxWaitMillis) {
        Map<String, JsonObject> completedTransfers = new HashMap<>();
        List<String> uncompletedTransfers = new ArrayList<>();
        uncompletedTransfers.addAll(transferTaskIds);
        long startTime = System.currentTimeMillis();
        while(!uncompletedTransfers.isEmpty()) {
            for (String transferTaskId : uncompletedTransfers) {
                log.info("Waiting for transfer: " + transferTaskId);
                JsonObject result = checkTransfer(baseUrl, token, transferTaskId);
                TransferTaskStatus tts = TransferTaskStatus.valueOf(result.get("result").getAsJsonObject().get("status").getAsString());
                if (terminalStates.contains(tts)) {
                    log.info("Transfer Done: " + transferTaskId + " Status: " + tts);
                    completedTransfers.put(transferTaskId, result);
                }
            }
            uncompletedTransfers.removeAll(completedTransfers.keySet());
            long elapsedTime = System.currentTimeMillis() - startTime;
            if(elapsedTime > maxWaitMillis) {
                Assert.fail("Timout waiting for transfers to complete");
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

}
