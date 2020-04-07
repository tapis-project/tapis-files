package edu.utexas.tacc.tapis.files.lib.workers;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.ConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.rabbitmq.*;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

public class FluxWithRabbitMQDemo {

    private static final String QUEUE = "demo.flux.child";
    private static final Logger log = LoggerFactory.getLogger(FluxWithRabbitMQDemo.class);

    private final Sender sender;
    private final Receiver receiver;
    private final ObjectMapper mapper = new ObjectMapper();


    public FluxWithRabbitMQDemo() {
        ConnectionFactory connectionFactory = new ConnectionFactory();
        connectionFactory.setUsername("dev");
        connectionFactory.setPassword("dev");
        connectionFactory.setVirtualHost("dev");
        connectionFactory.useNio();
        SenderOptions senderOptions = new SenderOptions()
                .connectionFactory(connectionFactory)
                .resourceManagementScheduler(Schedulers.boundedElastic());
        ReceiverOptions receiverOptions = new ReceiverOptions()
                .connectionFactory(connectionFactory)
                .connectionSubscriptionScheduler(Schedulers.boundedElastic());
        this.sender = RabbitFlux.createSender(senderOptions);
        this.receiver = RabbitFlux.createReceiver(receiverOptions);
    }

    private Mono<String> doWork(AcknowledgableDelivery message) {
        try {
            ObjectMapper mapper = new ObjectMapper();
            ChildMessage childMessage = mapper.readValue(message.getBody(), ChildMessage.class);
            int sleep = new Random().nextInt(5000) + 100;
            Thread.sleep(sleep);
            log.info("Receiver completed task {} in: {}", childMessage, sleep);
            return Mono.just(childMessage.getId() + "::" + sleep);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }


    public void run(int count) {
        ConsumeOptions options = new ConsumeOptions();
        options.qos(500);
        Flux<AcknowledgableDelivery> messages = receiver.consumeManualAck(QUEUE, options);
        messages.delaySubscription(sender.declareQueue(QueueSpecification.queue(QUEUE)))
                .groupBy((message) -> {
                    try {
                        ChildMessage m = mapper.readValue(message.getBody(), ChildMessage.class);
                        return m.getParentTaskId();
                    } catch (IOException e) {
                        e.printStackTrace();
                        return null;
                    }
                })
                .map(g ->  {
                    log.info(g.key().toString());
                    return g.parallel();
                })
                .subscribe((stream) -> {

                    stream
                            .runOn(Schedulers.newParallel("pool", 4))
                            .doOnError(m -> {
                                // sendToRetryQueue();
                                log.error(m.toString());
                            })
                            .doOnComplete( () -> {
                                log.info("Completed!");
                            })
                            .subscribe( (message) -> {
                                this.doWork(message);
                                message.ack();
                            });
                });

        AtomicInteger counter = new AtomicInteger(0);
        Flux<OutboundMessageResult> dataStream = sender.sendWithPublishConfirms(
                Flux.range(1, count)
                        .window(100)
                        .concatMap( (g) -> {
                            counter.getAndIncrement();
                            return g.map( (i) -> {
                                int parentTaskId = counter.get();
                                ChildMessage childMessage = new ChildMessage(parentTaskId, i);
                                OutboundMessage message = null;
                                try {
                                    String m = mapper.writeValueAsString(childMessage);
                                    message = new OutboundMessage("", QUEUE, m.getBytes(StandardCharsets.UTF_8));
                                } catch (Exception e) {
                                    e.printStackTrace();
                                }
                                return message;
                            });
                        }));


        sender.declareQueue(QueueSpecification.queue(QUEUE))
                .thenMany(dataStream)
                .doOnError(e -> log.error("Send failed" + e))
                .subscribe(m -> {
                    if (m != null) {
//                        log.info("Successfully sent message");
                    }
                });

        while (true) {
            try {
                Thread.sleep(10000);
            } catch (InterruptedException ex) {
                log.error("killed", ex);
            }
        }

    }

    public static void main(String[] args) throws Exception {
        int count = 500;
        FluxWithRabbitMQDemo sender = new FluxWithRabbitMQDemo();
        sender.run(count);
    }
}