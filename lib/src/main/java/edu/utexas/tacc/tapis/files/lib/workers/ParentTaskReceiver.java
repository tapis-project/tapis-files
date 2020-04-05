package edu.utexas.tacc.tapis.files.lib.workers;

import com.rabbitmq.client.ConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Schedulers;
import reactor.rabbitmq.*;

import java.util.ArrayList;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 *
 */
public class ParentTaskReceiver {

    private static final String QUEUE = "parent";
    private static final Logger LOGGER = LoggerFactory.getLogger(ParentTaskReceiver.class);

    private final Receiver receiver;
    private final Sender sender;

    public ParentTaskReceiver() {
        ConnectionFactory connectionFactory = new ConnectionFactory();
        connectionFactory.setUsername("dev");
        connectionFactory.setPassword("dev");
        connectionFactory.setVirtualHost("dev");
        connectionFactory.useNio();
        ReceiverOptions receiverOptions = new ReceiverOptions()
                .connectionFactory(connectionFactory)
                .connectionSubscriptionScheduler(Schedulers.newElastic("parent-receiver"));
        SenderOptions senderOptions = new SenderOptions()
                .connectionFactory(connectionFactory)
                .resourceManagementScheduler(Schedulers.newElastic("parent-sender"));
        this.receiver = RabbitFlux.createReceiver(receiverOptions);
        this.sender = RabbitFlux.createSender(senderOptions);
    }

    public Disposable consume(String queue) {

        ConsumeOptions options = new ConsumeOptions();
        options.qos(1);
        return receiver.consumeAutoAck(queue, options)
                .delaySubscription(sender.declareQueue(QueueSpecification.queue(queue)))
                .subscribe(m -> {
                    try {
                        String message = new String(m.getBody());
                        LOGGER.info("Received message {}", new String(m.getBody()));
//                        String queueName = String.format("parent.%s", message);
//                        ChildTaskReceiver child = new ChildTaskReceiver(queueName);
//                        child.consume();

//                        int sleep = new Random().nextInt(5000) + 100;
                        Thread.sleep(1000);
//                        Flux<String> files = Flux.just("file1.txt", "file3.txt", "file4.txt");
//                        Flux<OutboundMessageResult> confirmations = sender.sendWithPublishConfirms(
//                                files.map(file -> new OutboundMessage("", queueName, (message + "@" + file).getBytes())));

//                        sender.declareQueue(QueueSpecification.queue(queueName))
//                                .thenMany(confirmations)
//                                .doOnError(e -> LOGGER.error("Send failed", e))
//                                .subscribe(r -> {
//                                    if (r.isAck()) {
//                                        LOGGER.info("Message {} sent successfully", new String(r.getOutboundMessage().getBody()));
//                                    }
//                                });
                    } catch (Exception ex) {
                        LOGGER.error("error", ex);
                    }


                });
    }

    public void close() {
        this.sender.close();
        this.receiver.close();
    }

    public static void main(String[] args) throws Exception {
        ParentTaskReceiver receiver = new ParentTaskReceiver();
        Disposable disposable = receiver.consume(QUEUE);

    }

}