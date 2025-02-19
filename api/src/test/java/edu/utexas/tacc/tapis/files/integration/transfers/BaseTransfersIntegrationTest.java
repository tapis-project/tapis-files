package edu.utexas.tacc.tapis.files.integration.transfers;

import com.google.gson.JsonObject;
import edu.utexas.tacc.tapis.files.integration.transfers.configs.BaseTransfersIntegrationConfig;
import edu.utexas.tacc.tapis.files.integration.transfers.configs.CleanupConfig;
import edu.utexas.tacc.tapis.files.integration.transfers.configs.TransfersIntegrationTestConfig;
import edu.utexas.tacc.tapis.files.integration.transfers.configs.UploadFilesConfig;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.BeforeClass;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

abstract public class BaseTransfersIntegrationTest <T extends BaseTransfersIntegrationConfig> {
    Logger log = LoggerFactory.getLogger(BaseTransfersIntegrationTest.class);
    public static final String TRANSFERS_INTEGRATION_TEST_CONFIG = "IntegrationTestCommonConfig.json";
    private static final String SHA_PREFIX = "sha256:";
    private TransfersIntegrationTestConfig integrationConfig;
    private final String testConfigFileName;
    private ExecutorService defaultThreadPool = Executors.newFixedThreadPool(10);

    private Class<T> testConfigClass;

    private T testConfig;


    private final String runId;

    private String token;

    Map<Path, String> testFiles;
    private final IntegrationTestUtils testUtils;

    protected BaseTransfersIntegrationTest(Class<T> testConfigClass, String testConfigFileName) {
        testUtils = IntegrationTestUtils.instance;
        runId = UUID.randomUUID().toString();
        this.testConfigClass = testConfigClass;
        testFiles = new ConcurrentHashMap();
        this.testConfigFileName = testConfigFileName;
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

    @BeforeClass
    public void beforeClass() throws Exception {
        integrationConfig = testUtils.readTestConfig(TRANSFERS_INTEGRATION_TEST_CONFIG, TransfersIntegrationTestConfig.class);
        JsonObject testConfigs = testUtils.readTestConfig(testConfigFileName, JsonObject.class);
        testConfig = (T)testUtils.getGson().fromJson(testConfigs, testConfigClass);

        token = IntegrationTestUtils.instance.getToken(integrationConfig.getTokenUrl(), integrationConfig.getUsername(), integrationConfig.getPassword());
        cleanup();
        List<UploadFilesConfig> uploadFilesConfigs = testConfig.getUploadFiles();
        if(uploadFilesConfigs != null) {
            for (UploadFilesConfig uploadFilesConfig : uploadFilesConfigs) {
                log.info("Uploading files.  System: " + uploadFilesConfig.getUploadSystem() + " Path: " + uploadFilesConfig.getUploadPath());
                uploadFiles(uploadFilesConfig.getFilePrefix(), uploadFilesConfig);
            }
        }
    }

    public ExecutorService getThreadPool() {
        return defaultThreadPool;
    }

    public void cleanup() {
        List<CleanupConfig> cleanupConfigs = testConfig.getCleanup();
        if(cleanupConfigs != null) {
            for (CleanupConfig cleanupConfig : cleanupConfigs) {
                IntegrationTestUtils.instance.deletePath(integrationConfig.getBaseFilesUrl(), token, cleanupConfig.getSystem(),
                        cleanupConfig.getPath(), cleanupConfig.getPattern());
            }
        }
    }

    private void uploadFiles(String filePrefix, UploadFilesConfig uploadFilesConfig) throws Exception {
        final String filePrefixToUse = (StringUtils.isEmpty(filePrefix)) ? "integration_test_file_" : filePrefix;
        List<Future<String>> uploadFutures = new ArrayList<>();
        IntegrationTestUtils.instance.mkdir(integrationConfig.getBaseFilesUrl(), token, uploadFilesConfig.getUploadSystem(), uploadFilesConfig.getUploadPath());
        for(int i = 0; i < uploadFilesConfig.getCount(); i++) {
            Future<String> uploadFuture = getThreadPool().submit(new Callable<String>() {
                @Override
                public String call() throws Exception {
                    Path destinationPath = Path.of(filePrefixToUse + UUID.randomUUID());
                    String digest = IntegrationTestUtils.instance.uploadRandomFile(integrationConfig.getBaseFilesUrl(), token,
                            uploadFilesConfig.getUploadSystem(), Paths.get(uploadFilesConfig.getUploadPath().toString(),
                                    destinationPath.toString()), uploadFilesConfig.getSize(), true);
                    testFiles.put(destinationPath, digest);
                    return digest;
                }
            });
            uploadFutures.add(uploadFuture);
        }

        for(Future<String> uploadFuture : uploadFutures) {
            String digest = uploadFuture.get();
            log.info("Upload digest: " + digest);
        }
    }

}
