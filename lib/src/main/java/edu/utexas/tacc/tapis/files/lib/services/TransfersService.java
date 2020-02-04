package edu.utexas.tacc.tapis.files.lib.services;

import edu.utexas.tacc.tapis.files.lib.dao.transfers.FileTransfersDAO;
import edu.utexas.tacc.tapis.files.lib.exceptions.DAOException;
import edu.utexas.tacc.tapis.files.lib.exceptions.ServiceException;
import edu.utexas.tacc.tapis.files.lib.json.TapisObjectMapper;
import edu.utexas.tacc.tapis.files.lib.models.TransferTask;
import edu.utexas.tacc.tapis.files.lib.rabbit.RabbitMQConnection;
import org.jvnet.hk2.annotations.Service;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeoutException;

public class TransfersService {
    private Logger log = LoggerFactory.getLogger(TransfersService.class);

    private FileTransfersDAO dao;
    private final String EXCHANGE_NAME = "tapis.files";
    private final String QUEUE_NAME = "files.transfers";
    private Connection connection;
    private Channel channel;
    private static final ObjectMapper mapper = TapisObjectMapper.getMapper();

    public TransfersService() throws ServiceException {
        dao = new FileTransfersDAO();
        try {
            Connection conn = RabbitMQConnection.getInstance();
            connection = conn;
            channel = conn.createChannel();
            channel.queueDeclare(QUEUE_NAME, true, false, false, null);
        } catch (IOException ex) {
            log.error(ex.getMessage());
            throw new ServiceException(ex.getMessage());
        }
    }

    public TransferTask createTransfer(String username, String tenantId,
                                       String sourceSystemId, String sourcePath,
                                       String destinationSystemId, String destinationPath) throws ServiceException {

        log.info(username);
        TransferTask task = new TransferTask(tenantId, username, sourceSystemId, sourcePath, destinationSystemId, destinationPath);
        try {
            task = dao.createTransferTask(task);
        } catch (DAOException ex) {
            log.error("createTransfer", ex);
            throw new ServiceException(ex.getMessage());
        }
        log.info(task.toString());
        return task;
    }

    public void publishTransferTaskMessage(TransferTask task) throws ServiceException {
        try {
            String message  = mapper.writeValueAsString(task);
            channel.basicPublish("", QUEUE_NAME, null, message.getBytes(StandardCharsets.UTF_8));
        } catch (Exception ex) {
            log.error(ex.getMessage());
            throw new ServiceException(ex.getMessage());
        }

    }


}
