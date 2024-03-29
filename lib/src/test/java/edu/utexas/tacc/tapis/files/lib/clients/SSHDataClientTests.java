package edu.utexas.tacc.tapis.files.lib.clients;

import edu.utexas.tacc.tapis.files.lib.caches.SystemsCache;
import edu.utexas.tacc.tapis.systems.client.gen.model.TapisSystem;

import org.testng.annotations.Test;

@Test
public class SSHDataClientTests extends BaseDataClientTests<SSHDataClient> {

    private static final String JSON_TEST_PATH="edu/utexas/tacc/tapis/files/lib/clients/SSHDataClientTests.json";

    public SSHDataClientTests() {
        super(SSHDataClientTests.JSON_TEST_PATH);
    }

    @Test
    public void testClass() throws Exception {
        System.out.println("Put some tests in this class to that are ssh specific");
    }

    @Override
    public SSHDataClient createDataClient(String tenantName, String userName, TapisSystem system, SystemsCache systemsCache,
                                          String impersonationId, String sharedCtxGrantor) throws Exception {
        return new SSHDataClient(tenantName, userName, system, systemsCache, impersonationId, sharedCtxGrantor);
    }
}
