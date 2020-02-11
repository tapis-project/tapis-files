package edu.utexas.tacc.tapis.files.api;

import edu.utexas.tacc.tapis.files.api.models.TransferTaskRequest;
import edu.utexas.tacc.tapis.files.api.utils.TapisResponse;
import edu.utexas.tacc.tapis.files.lib.models.TransferTask;
import edu.utexas.tacc.tapis.files.lib.models.TransferTaskStatus;
import org.apache.commons.codec.Charsets;
import org.glassfish.jersey.test.JerseyTestNg;
import org.glassfish.jersey.test.TestProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import org.apache.commons.io.IOUtils;


import javax.ws.rs.client.Entity;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.UUID;

@Test(groups={"integration"})
public class ITestTransfersRoutes extends JerseyTestNg.ContainerPerClassTest {

    private Logger log = LoggerFactory.getLogger(ITestTransfersRoutes.class);
    private String user1jwt;
    private String user2jwt;
    private static class TransferTaskResponse extends TapisResponse<TransferTask>{}

    @Override
    protected Application configure() {
        enable(TestProperties.LOG_TRAFFIC);
        enable(TestProperties.DUMP_ENTITY);
        return new FilesApplication();
    }

    @AfterClass
    public void tearDown() throws Exception {
        super.tearDown();
    }

    @BeforeClass
    public void setUp() throws Exception {
        super.setUp();
        user1jwt = IOUtils.resourceToString("/user1jwt", Charsets.UTF_8);
        user2jwt = IOUtils.resourceToString("/user2jwt", Charsets.UTF_8);
    }

    /**
     * Helper method to create transfer tasks
     * @return
     */
    private TransferTask createTransferTask() {
        TransferTaskRequest payload = new TransferTaskRequest();
        payload.setSourceSystemId("sourceSystem");
        payload.setSourcePath("sourcePath");
        payload.setDestinationSystemId("destinationSystem");
        payload.setDestinationPath("destinationPath");

        Response createTaskResponse = target("/transfers")
                .request()
                .accept(MediaType.APPLICATION_JSON)
                .header("x-tapis-token", user1jwt)
                .post(Entity.json(payload));
        TransferTaskResponse t = createTaskResponse.readEntity(TransferTaskResponse.class);
        return t.getResult();
    }

    private TransferTask getTransferTask(String taskUUID) {
        TransferTaskResponse getTaskResponse = target("/transfers/" +taskUUID)
                .request()
                .accept(MediaType.APPLICATION_JSON)
                .header("x-tapis-token", user1jwt)
                .get(TransferTaskResponse.class);

        TransferTask task = getTaskResponse.getResult();
        return task;

    }

    @Test
    public void postTransferTask() {
        TransferTask newTask = createTransferTask();

        Assert.assertNotNull(newTask.getUuid());
        Assert.assertNotNull(newTask.getCreated());
        Assert.assertEquals(newTask.getSourceSystemId(), "sourceSystem");
        Assert.assertEquals(newTask.getSourcePath(), "sourcePath");
        Assert.assertEquals(newTask.getUsername(), "testuser1");
        Assert.assertEquals(newTask.getTenantId(), "dev");
        Assert.assertEquals(newTask.getStatus(), TransferTaskStatus.ACCEPTED.name());
    }

    @Test
    public void getTransferById() {

        TransferTask t = createTransferTask();

        TransferTask task = getTransferTask(t.getUuid().toString());

        Assert.assertEquals(t.getDestinationPath(), task.getDestinationPath());
        Assert.assertEquals(t.getDestinationSystemId(), task.getDestinationSystemId());
        Assert.assertEquals(t.getSourcePath(), task.getSourcePath());
        Assert.assertEquals(t.getSourceSystemId(), task.getSourceSystemId());
        Assert.assertNotNull(task.getUuid());
        Assert.assertNotNull(task.getCreated());
    }


    /**
     * This request should throw a 400 as the ValidUUID validator will fail
     */
    @Test
    public void getTransfer400() {

        Response response = target("/transfers/INVALID")
                .request()
                .accept(MediaType.APPLICATION_JSON)
                .header("x-tapis-token", user1jwt)
                .get();

        TransferTaskResponse data = response.readEntity(TransferTaskResponse.class);
        Assert.assertEquals(response.getStatus(), 400);
        Assert.assertEquals(data.getStatus(), "error");
    }

    /**
     * Valid UUID but not found should be 404
     */
    @Test
    public void getTransfer404() {
        String uuid = UUID.randomUUID().toString();
        Response response = target("/transfers/" + uuid)
                .request()
                .accept(MediaType.APPLICATION_JSON)
                .header("x-tapis-token", user1jwt)
                .get();

        TransferTaskResponse data = response.readEntity(TransferTaskResponse.class);
        Assert.assertEquals(response.getStatus(), 404);
        Assert.assertEquals(data.getStatus(), "error");
    }

    @Test
    public void deleteTransfer() {
        TransferTask t = createTransferTask();
        Response resp = target("/transfers/" + t.getUuid().toString())
                .request()
                .accept(MediaType.APPLICATION_JSON)
                .header("x-tapis-token", user1jwt)
                .delete();

        TransferTask task = getTransferTask(t.getUuid().toString());
        Assert.assertEquals(task.getStatus(), TransferTaskStatus.CANCELLED.name());
        Assert.assertEquals(resp.getStatus(), 200);

    }

}
