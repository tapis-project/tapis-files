package edu.utexas.tacc.tapis.files.lib.workers;

import com.rabbitmq.client.*;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeoutException;

import edu.utexas.tacc.tapis.files.lib.rabbit.RabbitMQConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.validation.constraints.NotNull;

public class ReceiveLogs {
    private static final Logger log = LoggerFactory.getLogger(ReceiveLogs.class);
    private static final String EXCHANGE_NAME = "tapis.files";
    private static final String QUEUE_NAME = "files.transfers";

    private static class TestWorker implements Runnable {

        Channel channel;
        Integer taskID;

        public TestWorker(@NotNull  Channel channel, @NotNull Integer taskID) {
            this.channel = channel;
            this.taskID = taskID;
        }

        @Override
        public void run() {
            log.info(String.format("Worker %s", taskID));
            try {
                channel.basicQos(1);
                Consumer consumer = new FileTransferConsumer(channel);
                channel.basicConsume(QUEUE_NAME, false, consumer);
            } catch (IOException  ex) {
                log.error("Error", ex);
            }
        }
    }

    public static void main(String[] argv) throws Exception {
        ExecutorService executorService = Executors.newFixedThreadPool(20);
        Connection connection = RabbitMQConnection.getInstance();

        for (int i=0; i<50; i++) {
            Channel channel = connection.createChannel();
            executorService.submit(new TestWorker(channel, i));
        }
    }
}