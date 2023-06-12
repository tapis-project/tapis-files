package edu.utexas.tacc.tapis.files.integration.transfers;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;
import com.google.gson.TypeAdapter;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonWriter;
import edu.utexas.tacc.tapis.files.integration.transfers.configs.BaseTransfersIntegrationConfig;
import edu.utexas.tacc.tapis.files.integration.transfers.configs.CleanupConfig;
import edu.utexas.tacc.tapis.files.integration.transfers.configs.TransfersIntegrationTestConfig;
import edu.utexas.tacc.tapis.files.integration.transfers.configs.UploadFilesConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.BeforeTest;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class BaseTransfersIntegrationTest <T extends BaseTransfersIntegrationConfig> {
    Logger log = LoggerFactory.getLogger(BaseTransfersIntegrationTest.class);
    private static final String TRANSFERS_INTEGRATION_TEST_CONFIG = "TransfersIntegrationTestConfig.json";
    private static final String SHA_PREFIX = "sha256:";
    private TransfersIntegrationTestConfig integrationConfig;

    private Class<T> testConfigClass;

    private T testConfig;


    private final String runId;

    private String token;

    Map<Path, String> testFiles;

    protected BaseTransfersIntegrationTest(Class<T> testConfigClass) {
        runId = UUID.randomUUID().toString();
        this.testConfigClass = testConfigClass;
        testFiles = new HashMap<>();
    }

    public T getTestConfig() {
        return testConfig;
    }

    public String getBaseFilesUrl() {
        return integrationConfig.getBaseFilesUrl();
    }

    public String getToken() {
        return token;
    }

    public Map<Path, String> getTestFiles() {
        return testFiles;
    }

    @BeforeTest
    public void beforeClass() throws Exception {
        integrationConfig = readTestConfig(TRANSFERS_INTEGRATION_TEST_CONFIG, TransfersIntegrationTestConfig.class);
        JsonObject testConfigs =  integrationConfig.getTestConfigs();
        testConfig = (T)getGson().fromJson(testConfigs.get(this.getClass().getSimpleName()), testConfigClass);

        token = TestUtils.instance.getToken(integrationConfig.getTokenUrl(), integrationConfig.getUsername(), integrationConfig.getPassword());
        cleanup();
        for(UploadFilesConfig uploadFilesConfig : testConfig.getUploadFiles()) {
            log.info("Uploading files.  System: " + uploadFilesConfig.getUploadSystem() + " Path: " + uploadFilesConfig.getUploadPath());
            uploadFiles(uploadFilesConfig);
        }

    }

    public void cleanup() {
        List<CleanupConfig> cleanupConfigs = testConfig.getCleanup();
        for(CleanupConfig cleanupConfig : cleanupConfigs) {
            TestUtils.instance.deletePath(integrationConfig.getBaseFilesUrl(), token, cleanupConfig.getSystem(), cleanupConfig.getPath());
        }
    }

    private <T> T readTestConfig(String fileName, Class<T> cls) throws Exception {
        InputStream configStream = this.getClass().getClassLoader().getResourceAsStream(fileName);

        try(Reader reader = new InputStreamReader(configStream)) {
            return getGson().fromJson(reader, cls);
        }
    }

    private void uploadFiles(UploadFilesConfig uploadFilesConfig) throws Exception {
        for(int i = 0; i < uploadFilesConfig.getCount(); i++) {
            Path destinationPath = Path.of("integration_test_file" + UUID.randomUUID());
            String digest = TestUtils.instance.uploadRandomFile(integrationConfig.getBaseFilesUrl(), token,
                    uploadFilesConfig.getUploadSystem(), Paths.get(uploadFilesConfig.getUploadPath().toString(),
                            destinationPath.toString()), uploadFilesConfig.getSize(), true);
            testFiles.put(destinationPath, digest);
        }
    }

    protected Gson getGson() {
        GsonBuilder gBuilder = new GsonBuilder();
        gBuilder.registerTypeAdapter(Path.class, new TypeAdapter<Path>() {
            @Override
            public void write(JsonWriter jsonWriter, Path path) throws IOException {
                jsonWriter.value(path.toString());
            }

            @Override
            public Path read(JsonReader jsonReader) throws IOException {
                String pathString = jsonReader.nextString();
                return Path.of(pathString);
            }
        });

        return gBuilder.create();
    }

}
