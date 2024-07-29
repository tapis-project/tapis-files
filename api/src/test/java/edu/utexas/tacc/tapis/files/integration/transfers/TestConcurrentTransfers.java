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
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class TestConcurrentTransfers extends BaseTransfersIntegrationTest<TestFileTransfersConfig> {
    private static final String TEST_CONFIG = "TestConcurrentTransfersConfig.json";
    private ExecutorService threadPool = null;
    class TransferInfo {
        private final TransfersConfig transfersConfig;
        private final Path filename;
        String transferId;

        public TransferInfo(TransfersConfig transfersConfig, Path filename) {
            this.filename = filename;
            this.transfersConfig = transfersConfig;
            this.transferId = transferId;
        }
        public String getTransferId() {
            return transferId;
        }

        public void setTransferId(String transferId) {
            this.transferId = transferId;
        }

        public TransfersConfig getTransfersConfig() {
            return transfersConfig;
        }

        public Path getFilename() {
            return filename;
        }
    }

    private class TransferCallable implements Callable<TransferInfo> {
        TransferInfo transferInfo;
        TransferCallable(TransfersConfig transfersConfig, Path filename) {
            this.transferInfo = new TransferInfo(transfersConfig, filename);
        }

        @Override
        public TransferInfo call() {
            TransfersConfig transfersConfig = this.transferInfo.getTransfersConfig();
            Path filename =  this.transferInfo.getFilename();
            IntegrationTestUtils.TransferDefinition transferDefinition = new IntegrationTestUtils.TransferDefinition();
            transferDefinition.setSourcePath(transfersConfig.getTapisSourcePath(filename));
            transferDefinition.setDestinationPath(transfersConfig.getTapisDestinationPath(filename));
            JsonObject tapisResult = IntegrationTestUtils.instance.transferFiles(getBaseFilesUrl(), getToken(), "integrationTestTransfer", transferDefinition);
            String transferId = getIdFromTransferResult(tapisResult);
            transferInfo.setTransferId(transferId);
            log.info("Submitted transfer request.  sourceId: " +
                    transfersConfig.getSourceSystem() + "  sourcePath: " + transfersConfig.getSourcePath() + " " +
                    "destinationSystem" + transfersConfig.getDestinationSystem() + "  destinationPath: " + transfersConfig.getDestinationPath() +
                    "  Id: " + transferId);
            return transferInfo;
        }
    }

    public TestConcurrentTransfers() {
        super(TestFileTransfersConfig.class, TEST_CONFIG);
    }
    @AfterTest
    public void afterClass() {
        cleanup();
    }

    @Override
    public ExecutorService getThreadPool() {
        if(threadPool == null) {
            synchronized (this) {
                if(threadPool == null) {
                    threadPool = Executors.newFixedThreadPool(getTestConfig().getMaxThreads());
                }
            }
        }
        return threadPool;
    }

    @Test
    public void testConcurrentTransfers() throws Exception {
        List<TransfersConfig> transfersConfigs = getTestConfig().getTransfers();
        List<TransferInfo> transferInfos = new ArrayList<>();
        List<Future<TransferInfo>> transferFutures = new ArrayList<>();


        // request all of the transfers
        for(TransfersConfig transfersConfig : transfersConfigs) {
            List<FileInfo> filesToTransfer = IntegrationTestUtils.instance.getListing(getBaseFilesUrl(), getToken(), transfersConfig.getSourceSystem(), transfersConfig.getSourcePath());
            for(FileInfo fileInfo : filesToTransfer) {
                Future<TransferInfo> transferFuture = getThreadPool().submit(new TransferCallable(transfersConfig, Path.of(fileInfo.getPath()).getFileName()));
                transferFutures.add(transferFuture);
            }
        }

        long transferStartTime = System.currentTimeMillis();
        List<String> transferIds = new ArrayList<>();
        // make a list of all of the transferIds
        for(Future<TransferInfo> transferFuture : transferFutures) {
            TransferInfo transferInfo = transferFuture.get();
            transferIds.add(transferInfo.getTransferId());
            transferInfos.add(transferInfo);
        }

        // wait for aall tranfer ids to complete
        IntegrationTestUtils.instance.waitForTransfers(getBaseFilesUrl(), getToken(), transferIds, getTestConfig().getTimeout(), threadPool, getTestConfig().getPollingIntervalMillis(), TimeUnit.MILLISECONDS);
        long transerDuration = System.currentTimeMillis() - transferStartTime;
        log.debug("Transfers took - total time: " + transerDuration + "   average time: " + transerDuration / transferFutures.size());

        // request download and validation of each file
        List<Future<Void>> downloadFutures = new ArrayList<>();
        for(TransferInfo transferInfo : transferInfos) {
            Future<Void> dowloadFuture = threadPool.submit(new Callable<Void>() {
                public Void call() throws Exception {
                    TransfersConfig transfersConfig = transferInfo.getTransfersConfig();
                    Path fileName = transferInfo.getFilename();
                    IntegrationTestUtils.instance.downloadAndVerify(getBaseFilesUrl(), getToken(),
                            transfersConfig.getDestinationSystem(), Path.of(transfersConfig.getDestinationPath().toString(), fileName.toString()), getTestFiles().get(fileName));
                    return null;
                }
            });
            downloadFutures.add(dowloadFuture);
        }

        // Wait for each to complete.  The get will throw and exception if the validation fails
        for(Future<Void> downloadFuture : downloadFutures) {
            downloadFuture.get();
        }
    }

    private String getIdFromTransferResult(JsonObject jsonObject) {
        return jsonObject.get("result").getAsJsonObject().get("uuid").getAsString();
    }
}
