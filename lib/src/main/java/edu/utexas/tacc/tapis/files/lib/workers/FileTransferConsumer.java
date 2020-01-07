package edu.utexas.tacc.tapis.files.lib.workers;


import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.*;
import edu.utexas.tacc.tapis.files.lib.dao.transfers.FileTransfersDAO;
import edu.utexas.tacc.tapis.files.lib.json.TapisObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Random;
import edu.utexas.tacc.tapis.files.lib.models.TransferTask;

public class FileTransferConsumer extends DefaultConsumer {
    private static final Logger log = LoggerFactory.getLogger(FileTransferConsumer.class);
    private final Channel channel;
    private final FileTransfersDAO dao;
    private final ObjectMapper mapper = TapisObjectMapper.getMapper();


    public FileTransferConsumer(Channel channel) {
        super(channel);
        this.channel = channel;
        this.dao = new FileTransfersDAO();
    }

    @Override
    public void handleDelivery(String consumerTag, Envelope envelope,
                               AMQP.BasicProperties properties, final byte[] body) throws IOException {
        long deliveryTag = envelope.getDeliveryTag();
        try {
            TransferTask task = mapper.readValue(body, TransferTask.class);
            int sleep = new Random().nextInt(5000) + 100;
            Thread.sleep(sleep);
            log.info("Task completed {} in {} seconds", task.getUuid(), sleep);
            channel.basicAck(deliveryTag, true);
        } catch (Exception e) {
            log.error("", e);
            channel.basicAck(deliveryTag, false);
        }
    }
}
