package edu.utexas.tacc.tapis.files.integration.transfers;

import com.google.gson.JsonObject;
import edu.utexas.tacc.tapis.files.integration.transfers.configs.BaseTransfersIntegrationConfig;
import edu.utexas.tacc.tapis.files.integration.transfers.configs.TransfersIntegrationTestConfig;
import edu.utexas.tacc.tapis.files.integration.transfers.configs.UploadFilesConfig;
import edu.utexas.tacc.tapis.shared.utils.TapisGsonUtils;
import org.testng.annotations.BeforeTest;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class BaseTransfersIntegrationTest <T extends BaseTransfersIntegrationConfig> {
    private static final String TRANSFERS_INTEGRATION_TEST_CONFIG = "TransfersIntegrationTestConfig.json";
    private static final String SHA_PREFIX = "sha256:";
    private TransfersIntegrationTestConfig integrationConfig;

    private Class<T> testConfigClass;

    private T testConfig;

    private Path tmpDir;

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

    public Path getTmpDir() {
        return tmpDir;
    }

    public Map<Path, String> getTestFiles() {
        return testFiles;
    }

    @BeforeTest
    public void beforeClass() throws Exception {
        integrationConfig = readTestConfig(TRANSFERS_INTEGRATION_TEST_CONFIG, TransfersIntegrationTestConfig.class);
        JsonObject testConfigs =  integrationConfig.getTestConfigs();
        testConfig = (T)TapisGsonUtils.getGson().fromJson(testConfigs.get(this.getClass().getSimpleName()), testConfigClass);

        cleanup();
        tmpDir = Files.createDirectory(Path.of(integrationConfig.getTmpDir(), runId));
/*
        for (CreateLocalFilesConfig createLocalFilesConfig : testConfig.getCreateLocalFilesConfig()) {
            TestUtils.instance.createDigestedLocalFiles(tmpDir,
                    createLocalFilesConfig.getCount(), createLocalFilesConfig.getSize(), true);
        }

 */
        token = TestUtils.instance.getToken(integrationConfig.getTokenUrl(), integrationConfig.getUsername(), integrationConfig.getPassword());
        for(UploadFilesConfig uploadFilesConfig : testConfig.getUploadFilesConfig()) {
            uploadFiles(uploadFilesConfig);
        }

    }

    public void cleanup() {
    }

    private <T> T readTestConfig(String fileName, Class<T> cls) throws Exception {
        InputStream configStream = this.getClass().getClassLoader().getResourceAsStream(fileName);

        try(Reader reader = new InputStreamReader(configStream)) {
            return TapisGsonUtils.getGson().fromJson(reader, cls);
        }
    }

    private void uploadFiles(UploadFilesConfig uploadFilesConfig) throws Exception {
        for(int i = 0; i < uploadFilesConfig.getCount(); i++) {
            Path destinationPath = Path.of("integration_test_file" + UUID.randomUUID());
            String digest = TestUtils.instance.uploadRandomFile(integrationConfig.getBaseFilesUrl(), token,
                    uploadFilesConfig.getUploadSystem(), Paths.get(uploadFilesConfig.getUploadPath(),
                            destinationPath.toString()), uploadFilesConfig.getSize(), true);
            testFiles.put(destinationPath, digest);
        }
    }

}
