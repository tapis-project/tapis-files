package edu.utexas.tacc.tapis.files.lib.clients;

import edu.utexas.tacc.tapis.files.lib.caches.SystemsCache;
import edu.utexas.tacc.tapis.shared.ssh.SshSessionPool;
import edu.utexas.tacc.tapis.systems.client.gen.model.TapisSystem;

import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

@Test(groups = "integration")
public class SSHDataClientTests extends BaseDataClientTests<SSHDataClient> {

    private static final String JSON_TEST_PATH="edu/utexas/tacc/tapis/files/lib/clients/TestSystems.json";

    public SSHDataClientTests() {
        super(SSHDataClientTests.JSON_TEST_PATH);
    }

    @BeforeClass
    public void beforeClass() {
        if(SshSessionPool.getInstance() == null) {
            SshSessionPool.init();
        }
    }

    @Test
    public void testClass() throws Exception {
        System.out.println("Put some tests in this class to that are ssh specific");
    }

    @Override
    protected String getConfigSection() {
        return "ssh_system";
    }

    @Override
    public SSHDataClient createDataClient(String tenantName, String userName, TapisSystem system, SystemsCache systemsCache,
                                          String impersonationId, String sharedCtxGrantor) throws Exception {
        return new SSHDataClient(tenantName, userName, system, systemsCache, impersonationId, sharedCtxGrantor);
    }
}
