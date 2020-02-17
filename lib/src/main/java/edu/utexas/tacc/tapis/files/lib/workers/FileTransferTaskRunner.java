package edu.utexas.tacc.tapis.files.lib.workers;

import com.rabbitmq.client.Channel;
import edu.utexas.tacc.tapis.files.lib.models.TransferTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Random;

public class FileTransferTaskRunner implements Runnable {
    private static final Logger log = LoggerFactory.getLogger(FileTransferTaskRunner.class);
    private TransferTask task;
    private Channel channel;
    private long deliveryTag;

    public FileTransferTaskRunner(Channel c, long deliveryTag, TransferTask t) {
        this.task = t;
        this.channel = c;
        this.deliveryTag = deliveryTag;
    }

    @Override
    public void run() {
        try {
            int sleep = new Random().nextInt(5000) + 100;
            Thread.sleep(sleep);
            log.info("Worker: --- Task {} completed in {} seconds", task.getUuid(), sleep);
            channel.basicAck(deliveryTag, false);
        } catch (Exception ex) {
        }
    }
}
