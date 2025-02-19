package edu.utexas.tacc.tapis.files.integration.transfers;

import com.google.gson.JsonObject;
import edu.utexas.tacc.tapis.files.integration.transfers.configs.TestFileTransfersConfig;
import edu.utexas.tacc.tapis.files.integration.transfers.configs.TransfersConfig;
import edu.utexas.tacc.tapis.files.lib.models.FileInfo;
import org.apache.commons.lang3.StringUtils;
import org.testng.annotations.AfterTest;
import org.testng.annotations.Test;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

@Test(groups = {"integration"})
public class TestWeirdPathTransfers extends BaseTransfersIntegrationTest<TestFileTransfersConfig> {
    private static final String TEST_CONFIG = "TestWeirdPathTransfersConfig.json";

    public TestWeirdPathTransfers() {
        super(TestFileTransfersConfig.class, TEST_CONFIG);
    }

    @AfterTest
    public void afterClass() {
        cleanup();
    }

    /**
     * Test what happens when we use "." as the destination of a file transfer.
     *
     * Due to the nature of these paths, they will be outside of the normal integration directory structure, and should be
     * cleaned up carefully!!
     */
    @Test
    public void testTransfers() throws Exception {
        TestFileTransfersConfig config = getTestConfig();

        for(var transfersConfig : config.getTransfers()) {
            Path sourceFileName = transfersConfig.getSourceFileName();
            if(sourceFileName != null && !StringUtils.isEmpty(sourceFileName.toString())) {
                // transferring a single file
                List<String> transferTasks = new ArrayList<>();
                transferTasks.add(transferSingleFile(transfersConfig));
                IntegrationTestUtils.instance.waitForTransfers(getBaseFilesUrl(), getToken(), transferTasks, getTestConfig().getTimeout(), Executors.newSingleThreadExecutor(), getTestConfig().getPollingIntervalMillis(), TimeUnit.MILLISECONDS);
                transferTasks.clear();
                Path fileName = sourceFileName.getFileName();
                IntegrationTestUtils.instance.downloadAndVerify(getBaseFilesUrl(), getToken(),
                        transfersConfig.getDestinationSystem(), Path.of(transfersConfig.getDestinationPath().toString(), fileName.toString()), transfersConfig.getSourceSHA());
            } else {
                List<FileInfo> filesToTransfer = IntegrationTestUtils.instance.getListing(getBaseFilesUrl(), getToken(), transfersConfig.getSourceSystem(), transfersConfig.getSourcePath());
                List<String> transferTasks = null;
                transferTasks = doIndividualTransfer(transfersConfig, filesToTransfer);
                IntegrationTestUtils.instance.waitForTransfers(getBaseFilesUrl(), getToken(), transferTasks, getTestConfig().getTimeout(), Executors.newSingleThreadExecutor(), getTestConfig().getPollingIntervalMillis(), TimeUnit.MILLISECONDS);
                transferTasks.clear();
                for(FileInfo fileInfo : filesToTransfer) {
                    Path fileName = Path.of(fileInfo.getPath()).getFileName();
                    // download each file from the destination, and verify that is identical to the source.
                    IntegrationTestUtils.instance.downloadAndVerify(getBaseFilesUrl(), getToken(),
                            transfersConfig.getDestinationSystem(), Path.of(transfersConfig.getDestinationPath().toString(), fileName.toString()), getTestFiles().get(fileName));
                }
            }
        }
    }

    private String transferSingleFile(TransfersConfig transfersConfig) {
        // transfer file from source to destination
        IntegrationTestUtils.TransferDefinition transferDefinition = new IntegrationTestUtils.TransferDefinition();
        Path fileName = transfersConfig.getSourceFileName().getFileName();
        transferDefinition.setSourcePath(transfersConfig.getTapisSourcePath(fileName));
        transferDefinition.setDestinationPath(transfersConfig.getTapisDestinationPath(fileName));
        JsonObject tapisResult = IntegrationTestUtils.instance.transferFiles(getBaseFilesUrl(), getToken(), "integrationTestTransfer", transferDefinition);
        return getIdFromTransferResult(tapisResult);
    }
    private List<String> doIndividualTransfer(TransfersConfig transfersConfig, List<FileInfo> filesToTransfer) {
        List<String> transferTasks = new ArrayList<>();
        // for each transfer, iterate through all test files
        for(FileInfo fileInfo : filesToTransfer) {
            // transfer each file from source to destination
            IntegrationTestUtils.TransferDefinition transferDefinition = new IntegrationTestUtils.TransferDefinition();
            Path fileName = Path.of(fileInfo.getPath()).getFileName();
            transferDefinition.setSourcePath(transfersConfig.getTapisSourcePath(fileName));
            transferDefinition.setDestinationPath(transfersConfig.getTapisDestinationPath(fileName));
            JsonObject tapisResult = IntegrationTestUtils.instance.transferFiles(getBaseFilesUrl(), getToken(), "integrationTestTransfer", transferDefinition);
            transferTasks.add(getIdFromTransferResult(tapisResult));
        }
        return transferTasks;
    }

    private String getIdFromTransferResult(JsonObject jsonObject) {
        return jsonObject.get("result").getAsJsonObject().get("uuid").getAsString();
    }
}
