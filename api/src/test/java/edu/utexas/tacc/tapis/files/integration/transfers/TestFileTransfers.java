package edu.utexas.tacc.tapis.files.integration.transfers;

import com.google.gson.JsonObject;
import edu.utexas.tacc.tapis.files.integration.transfers.configs.TestFileTransfersConfig;
import edu.utexas.tacc.tapis.files.integration.transfers.configs.TransfersConfig;
import edu.utexas.tacc.tapis.files.lib.models.TransferTask;
import edu.utexas.tacc.tapis.sharedapi.responses.TapisResponse;
import io.swagger.v3.core.util.Json;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterTest;
import org.testng.annotations.Test;

import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Collections;
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
        List<String> transferTasks = new ArrayList<>();
        List<TransfersConfig> transfersConfigs = getTransfersConfig();
        for(TransfersConfig transfersConfig : transfersConfigs) {
            // for each transfer, iterate through all test files
            for(Path file : getTestFiles().keySet()) {
                // transfer each file from source to destination
                TestUtils.TransferDefinition transferDefinition = new TestUtils.TransferDefinition();
                transferDefinition.setSourcePath(transfersConfig.getTapisSourcePath(file.toString()));
                transferDefinition.setDestinationPath(transfersConfig.getTapisDestinationPath(file.toString()));
                JsonObject tapisResult = TestUtils.instance.transferFiles(getBaseFilesUrl(), getToken(), "integrationTestTransfer", transferDefinition);
                transferTasks.add(getIdFromTransferResult(tapisResult));
            }
            TestUtils.instance.waitForTransfers(getBaseFilesUrl(), getToken(), transferTasks, 10000);
            transferTasks.clear();
            for(Path file : getTestFiles().keySet()) {
                // download each file from the destination, and verify that is identical to the source.
                TestUtils.instance.downloadAndVerify(getBaseFilesUrl(), getToken(),
                        transfersConfig.getDestinationSystem(), Path.of(transfersConfig.getDestinationPath().toString(), file.toString()), getTestFiles().get(file));
            }

        }
    }

    private String getIdFromTransferResult(JsonObject jsonObject) {
        return jsonObject.get("result").getAsJsonObject().get("uuid").getAsString();
    }
    private List<TransfersConfig> getTransfersConfig() {
        return getTestConfig().getTransfersConfig();
    }
}
