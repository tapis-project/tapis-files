package edu.utexas.tacc.tapis.files.lib.workers;


import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.*;
import edu.utexas.tacc.tapis.files.lib.dao.transfers.FileTransfersDAO;
import edu.utexas.tacc.tapis.files.lib.json.TapisObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Random;
import java.util.concurrent.ExecutorService;

import edu.utexas.tacc.tapis.files.lib.models.TransferTask;

public class FileTransferConsumer extends DefaultConsumer {

    private static final Logger log = LoggerFactory.getLogger(FileTransferConsumer.class);
    private final Channel channel;
    private final ExecutorService executorService;
    private final ObjectMapper mapper = TapisObjectMapper.getMapper();
    private final String QUEUE_NAME = "tapis.files.transfers";

    public FileTransferConsumer(Channel channel, ExecutorService threadExecutor) throws IOException {
        super(channel);
        this.channel = channel;
        this.executorService = threadExecutor;
        channel.basicQos(1);
        channel.basicConsume(QUEUE_NAME, false, this);
    }


    @Override
    public void handleDelivery(String consumerTag,
                               Envelope envelope,
                               AMQP.BasicProperties properties,
                               byte[] body) throws IOException {
        long deliveryTag = envelope.getDeliveryTag();
        TransferTask task = mapper.readValue(body, TransferTask.class);
        Runnable taskRunner = new FileTransferTaskRunner(channel, deliveryTag, task);
        executorService.submit(taskRunner);

    }
}
