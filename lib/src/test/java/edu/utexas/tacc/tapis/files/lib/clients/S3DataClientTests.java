package edu.utexas.tacc.tapis.files.lib.clients;

import edu.utexas.tacc.tapis.files.lib.caches.SystemsCache;
import edu.utexas.tacc.tapis.systems.client.gen.model.TapisSystem;
import org.apache.commons.lang3.NotImplementedException;
import org.testng.annotations.Test;

public class S3DataClientTests extends BaseDataClientTests<S3DataClient> {
    private static final String JSON_TEST_PATH="edu/utexas/tacc/tapis/files/lib/clients/S3DataClientTests.json";

    public S3DataClientTests() {
        super(S3DataClientTests.JSON_TEST_PATH);
    }

    @Test
    public void testClass() throws Exception {
        System.out.println("Put some tests in this class to that are S3 specific");
    }
    @Override
    public S3DataClient createDataClient(String tenantName, String userName, TapisSystem system, SystemsCache systemsCache,
                                            String impersonationId, String sharedCtxGrantor) throws Exception {
        return new S3DataClient(tenantName, userName, system);
    }
}
