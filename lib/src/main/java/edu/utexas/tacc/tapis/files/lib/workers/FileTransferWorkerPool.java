package edu.utexas.tacc.tapis.files.lib.workers;

import com.rabbitmq.client.*;

import java.io.IOException;
import java.util.concurrent.*;

import edu.utexas.tacc.tapis.files.lib.rabbit.RabbitMQConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.validation.constraints.NotNull;

public class FileTransferWorkerPool {

    private static int NUM_THREADS = 200;

    private static final Logger log = LoggerFactory.getLogger(FileTransferWorkerPool.class);
    public static void main(String[] argv) throws Exception {
        ExecutorService executorService = Executors.newFixedThreadPool(NUM_THREADS);
        Connection connection = RabbitMQConnection.getInstance();


        for (var i = 0; i < NUM_THREADS; i++) {
            Channel channel = connection.createChannel();
            FileTransferConsumer worker = new FileTransferConsumer(channel, executorService);
        }

        while (true) {
            Thread.sleep(1000);
        }
    }
}