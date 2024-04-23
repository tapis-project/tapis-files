package edu.utexas.tacc.tapis.files.integration.transfers;

import com.google.gson.JsonObject;
import edu.utexas.tacc.tapis.files.integration.transfers.configs.TestFileTransfersConfig;
import edu.utexas.tacc.tapis.files.integration.transfers.configs.TransfersConfig;
import edu.utexas.tacc.tapis.files.lib.models.FileInfo;
import org.testng.annotations.AfterTest;
import org.testng.annotations.Test;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

@Test(groups = {"integration"})
public class TestMultiPathFileTransfers extends BaseTransfersIntegrationTest<TestFileTransfersConfig> {
    private static final String TEST_CONFIG = "TestMultiPathFileTransfersConfig.json";
    private static final String TEMP_DIR_PREFIX = "TestMultiPathFileTransfers_";

    public TestMultiPathFileTransfers() {
        super(TestFileTransfersConfig.class, TEST_CONFIG);
    }
    @AfterTest
    public void afterClass() {
        cleanup();
    }

    @Test
    public void testTransferFiles() throws Exception {
        List<TransfersConfig> transfersConfigs = getTransfersConfig();
        for(TransfersConfig transfersConfig : transfersConfigs) {
            List<FileInfo> filesToTransfer = IntegrationTestUtils.instance.getListing(getBaseFilesUrl(), getToken(), transfersConfig.getSourceSystem(), transfersConfig.getSourcePath());
            List<String> transferTasks = null;
            transferTasks = doBatchTransfer(transfersConfig, filesToTransfer);
            IntegrationTestUtils.instance.waitForTransfers(getBaseFilesUrl(), getToken(), transferTasks, getTestConfig().getTimeout());
            transferTasks.clear();
            for(FileInfo fileInfo : filesToTransfer) {
                Path fileName = Path.of(fileInfo.getPath()).getFileName();
                // download each file from the destination, and verify that is identical to the source.
                IntegrationTestUtils.instance.downloadAndVerify(getBaseFilesUrl(), getToken(),
                        transfersConfig.getDestinationSystem(), Path.of(transfersConfig.getDestinationPath().toString(), fileName.toString()), getTestFiles().get(fileName));
            }

        }
    }

    private List<String> doBatchTransfer(TransfersConfig transfersConfig, List<FileInfo> filesToTransfer) {
        List<String> transferTasks = new ArrayList<>();
        List<IntegrationTestUtils.TransferDefinition> transferDefinitions = new ArrayList<>();
        // for each transfer, iterate through all test files
        for(FileInfo fileInfo : filesToTransfer) {
            // transfer each file from source to destination
            IntegrationTestUtils.TransferDefinition transferDefinition = new IntegrationTestUtils.TransferDefinition();
            Path fileName = Path.of(fileInfo.getPath()).getFileName();
            transferDefinition.setSourcePath(transfersConfig.getTapisSourcePath(fileName));
            transferDefinition.setDestinationPath(transfersConfig.getTapisDestinationPath(fileName));
            transferDefinitions.add(transferDefinition);
        }
        JsonObject tapisResult = IntegrationTestUtils.instance.transferFiles(getBaseFilesUrl(), getToken(), "integrationTestTransfer", transferDefinitions.toArray(new IntegrationTestUtils.TransferDefinition[transferDefinitions.size()]));
        transferTasks.add(getIdFromTransferResult(tapisResult));

        return transferTasks;
    }

    private String getIdFromTransferResult(JsonObject jsonObject) {
        return jsonObject.get("result").getAsJsonObject().get("uuid").getAsString();
    }
    private List<TransfersConfig> getTransfersConfig() {
        return getTestConfig().getTransfers();
    }
}
