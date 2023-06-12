package edu.utexas.tacc.tapis.files.integration.transfers;

import com.google.gson.JsonObject;
import edu.utexas.tacc.tapis.files.integration.transfers.configs.TestFileTransfersConfig;
import edu.utexas.tacc.tapis.files.integration.transfers.configs.TransfersConfig;
import edu.utexas.tacc.tapis.files.lib.models.FileInfo;
import org.testng.annotations.AfterTest;
import org.testng.annotations.Test;

import java.io.IOException;
import java.nio.file.Path;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;

public class TestFileTransfers extends BaseTransfersIntegrationTest<TestFileTransfersConfig> {
    private static final String TEMP_DIR_PREFIX = "TestFileTransfers_";

    public TestFileTransfers() {
        super(TestFileTransfersConfig.class);
    }
    @AfterTest
    public void afterClass() throws IOException {
        cleanup();
    }

    @Test
    public void testTransferFilesIndividually() throws Exception {
        List<TransfersConfig> transfersConfigs = getTransfersConfig();
        for(TransfersConfig transfersConfig : transfersConfigs) {
            List<FileInfo> filesToTransfer = TestUtils.instance.getListing(getBaseFilesUrl(), getToken(), transfersConfig.getSourceSystem(), transfersConfig.getSourcePath());
            List<String> transferTasks = null;
            if(transfersConfig.isBatch()) {
                transferTasks = doBatchTransfer(transfersConfig, filesToTransfer);
            } else {
                transferTasks = doIndividualTransfer(transfersConfig, filesToTransfer);
            }
            TestUtils.instance.waitForTransfers(getBaseFilesUrl(), getToken(), transferTasks, transfersConfig.getTimeout());
            transferTasks.clear();
            for(FileInfo fileInfo : filesToTransfer) {
                Path fileName = Path.of(fileInfo.getPath()).getFileName();
                // download each file from the destination, and verify that is identical to the source.
                TestUtils.instance.downloadAndVerify(getBaseFilesUrl(), getToken(),
                        transfersConfig.getDestinationSystem(), Path.of(transfersConfig.getDestinationPath().toString(), fileName.toString()), getTestFiles().get(fileName));
            }

        }
    }

    private List<String> doIndividualTransfer(TransfersConfig transfersConfig, List<FileInfo> filesToTransfer) throws IOException, NoSuchAlgorithmException {
        List<String> transferTasks = new ArrayList<>();
        // for each transfer, iterate through all test files
        for(FileInfo fileInfo : filesToTransfer) {
            // transfer each file from source to destination
            TestUtils.TransferDefinition transferDefinition = new TestUtils.TransferDefinition();
            Path fileName = Path.of(fileInfo.getPath()).getFileName();
            transferDefinition.setSourcePath(transfersConfig.getTapisSourcePath(fileName));
            transferDefinition.setDestinationPath(transfersConfig.getTapisDestinationPath(fileName));
            JsonObject tapisResult = TestUtils.instance.transferFiles(getBaseFilesUrl(), getToken(), "integrationTestTransfer", transferDefinition);
            transferTasks.add(getIdFromTransferResult(tapisResult));
        }
        return transferTasks;
    }

    private List<String> doBatchTransfer(TransfersConfig transfersConfig, List<FileInfo> filesToTransfer) throws IOException, NoSuchAlgorithmException {
        List<String> transferTasks = new ArrayList<>();
        List<TestUtils.TransferDefinition> transferDefinitions = new ArrayList<>();
        // for each transfer, iterate through all test files
        for(FileInfo fileInfo : filesToTransfer) {
            // transfer each file from source to destination
            TestUtils.TransferDefinition transferDefinition = new TestUtils.TransferDefinition();
            Path fileName = Path.of(fileInfo.getPath()).getFileName();
            transferDefinition.setSourcePath(transfersConfig.getTapisSourcePath(fileName));
            transferDefinition.setDestinationPath(transfersConfig.getTapisDestinationPath(fileName));
            transferDefinitions.add(transferDefinition);
        }
        JsonObject tapisResult = TestUtils.instance.transferFiles(getBaseFilesUrl(), getToken(), "integrationTestTransfer", transferDefinitions.toArray(new TestUtils.TransferDefinition[transferDefinitions.size()]));
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
