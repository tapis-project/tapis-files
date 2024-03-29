package edu.utexas.tacc.tapis.files.lib.clients;

import edu.utexas.tacc.tapis.files.lib.caches.SystemsCache;
import edu.utexas.tacc.tapis.files.test.RandomByteInputStream;
import edu.utexas.tacc.tapis.systems.client.gen.model.TapisSystem;
import org.testng.annotations.Test;

import java.io.FileOutputStream;

public class IrodsDataClientTests extends BaseDataClientTests<IrodsDataClient> {
    private static final String JSON_TEST_PATH="edu/utexas/tacc/tapis/files/lib/clients/IrodsDataClientTests.json";

    public IrodsDataClientTests() {
        super(IrodsDataClientTests.JSON_TEST_PATH);
    }

    @Test
    public void testClass() throws Exception {
        System.out.println("Put some tests in this class to that are irods specific");
    }
    @Override
    public IrodsDataClient createDataClient(String tenantName, String userName, TapisSystem system, SystemsCache systemsCache,
                                            String impersonationId, String sharedCtxGrantor) throws Exception {
        return new IrodsDataClient(tenantName, userName, system);
    }
}
