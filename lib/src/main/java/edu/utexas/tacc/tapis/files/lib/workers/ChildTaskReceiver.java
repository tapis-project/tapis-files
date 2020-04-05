package edu.utexas.tacc.tapis.files.lib.workers;

import com.rabbitmq.client.ConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Disposable;
import reactor.core.scheduler.Schedulers;
import reactor.rabbitmq.*;

import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 *
 */
public class ChildTaskReceiver {

    private static final Logger LOGGER = LoggerFactory.getLogger(ChildTaskReceiver.class);

    private final Receiver receiver;
    private final Sender sender;
    private final String queueName;

    public ChildTaskReceiver(String queueName) {
        ConnectionFactory connectionFactory = new ConnectionFactory();
        connectionFactory.setUsername("dev");
        connectionFactory.setPassword("dev");
        connectionFactory.setVirtualHost("dev");
        connectionFactory.useNio();
        ReceiverOptions receiverOptions = new ReceiverOptions()
                .connectionFactory(connectionFactory)
                .connectionSubscriptionScheduler(Schedulers.boundedElastic());
        SenderOptions senderOptions = new SenderOptions().connectionFactory(connectionFactory);
        this.receiver = RabbitFlux.createReceiver(receiverOptions);
        this.sender = RabbitFlux.createSender(senderOptions);
        this.queueName = queueName;
    }

    public Disposable consume() {
        ConsumeOptions options = new ConsumeOptions();
        options.qos(1);
        return receiver.consumeAutoAck(queueName, options)
                .delaySubscription(sender.declareQueue(QueueSpecification.queue(queueName)))
                .subscribe(m -> {
                    try {
                        LOGGER.info("Child received message {}", new String(m.getBody()));
//                        int sleep = new Random().nextInt(5000) + 100;
//                        Thread.sleep(sleep);
                        Thread.sleep(5000);
                        LOGGER.info("Child completed task {} in {} sec", new String(m.getBody()), 5000);
                    } catch (Exception ex) {
                        LOGGER.error("error");
                    }
                });
    }

    public void close() {
        this.sender.close();
        this.receiver.close();
    }

}