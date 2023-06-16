package edu.utexas.tacc.tapis.files.integration.transfers;

import com.google.gson.JsonObject;
import edu.utexas.tacc.tapis.files.integration.transfers.configs.TestFileTransfersConfig;
import edu.utexas.tacc.tapis.files.integration.transfers.configs.TransfersConfig;
import edu.utexas.tacc.tapis.files.lib.models.FileInfo;
import org.testng.annotations.AfterTest;
import org.testng.annotations.Test;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

public class TestConcurrentTransfers extends BaseTransfersIntegrationTest<TestFileTransfersConfig> {
    private static final String TEMP_DIR_PREFIX = "TestFileTransfers_";

    private class TransferThread extends Thread {

        TransfersConfig transfersConfig;
        Path filename;
        String transferId;

        TransferThread(TransfersConfig transfersConfig, Path filename) {
            this.filename = filename;
            this.transfersConfig = transfersConfig;
        }

        @Override
        public void run() {
            TestUtils.TransferDefinition transferDefinition = new TestUtils.TransferDefinition();
            transferDefinition.setSourcePath(transfersConfig.getTapisSourcePath(filename));
            transferDefinition.setDestinationPath(transfersConfig.getTapisDestinationPath(filename));
            JsonObject tapisResult = TestUtils.instance.transferFiles(getBaseFilesUrl(), getToken(), "integrationTestTransfer", transferDefinition);
            transferId = getIdFromTransferResult(tapisResult);
            log.info("Submitted transfer request.  sourceId: " +
                    transfersConfig.getSourceSystem() + "  sourcePath: " + transfersConfig.getSourcePath() + " " +
                    "destinationSystem" + transfersConfig.getDestinationSystem() + "  destinationPath: " + transfersConfig.getDestinationPath() +
                    "  Id: " + transferId);
        }

        public TransfersConfig getTransfersConfig() {
            return transfersConfig;
        }

        public Path getFilename() {
            return filename;
        }

        public String getTransferId() {
            return transferId;
        }
    }

    public TestConcurrentTransfers() {
        super(TestFileTransfersConfig.class);
    }
    @AfterTest
    public void afterClass() throws IOException {
        cleanup();
    }

    @Test
    public void testConcurrentTransfers() throws Exception {
        List<TransfersConfig> transfersConfigs = getTestConfig().getTransfers();
        List<String> transferTasks = new ArrayList<>();
        List<TransferThread> transferThreads = new ArrayList<>();

        for(TransfersConfig transfersConfig : transfersConfigs) {
            List<FileInfo> filesToTransfer = TestUtils.instance.getListing(getBaseFilesUrl(), getToken(), transfersConfig.getSourceSystem(), transfersConfig.getSourcePath());
            for(FileInfo fileInfo : filesToTransfer) {
                TransferThread t = new TransferThread(transfersConfig, Path.of(fileInfo.getPath()).getFileName());
                t.start();
                transferThreads.add(t);
            }
        }

        for(Thread t : transferThreads) {
            t.join();
        }


        for(TransferThread transferThread : transferThreads) {
            TransfersConfig transfersConfig = transferThread.getTransfersConfig();
            transferTasks.add(transferThread.getTransferId());
            TestUtils.instance.waitForTransfers(getBaseFilesUrl(), getToken(), transferTasks, transfersConfig.getTimeout());
            transferTasks.clear();

            Path fileName = transferThread.getFilename();
            // download each file from the destination, and verify that is identical to the source.
            TestUtils.instance.downloadAndVerify(getBaseFilesUrl(), getToken(),
                    transfersConfig.getDestinationSystem(), Path.of(transfersConfig.getDestinationPath().toString(), fileName.toString()), getTestFiles().get(fileName));

        }
    }

    private String getIdFromTransferResult(JsonObject jsonObject) {
        return jsonObject.get("result").getAsJsonObject().get("uuid").getAsString();
    }
}
