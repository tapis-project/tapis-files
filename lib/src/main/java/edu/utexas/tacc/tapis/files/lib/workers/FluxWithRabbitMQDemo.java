package edu.utexas.tacc.tapis.files.lib.workers;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Delivery;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.rabbitmq.*;

import java.util.Random;
import java.util.stream.Collectors;

public class FluxWithRabbitMQDemo {

    private static final String QUEUE = "demo.flux";
    private static final Logger log = LoggerFactory.getLogger(FluxWithRabbitMQDemo.class);

    private final Sender sender;
    private final Receiver receiver;

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

    private Mono<String> doWork(Delivery message) {
        try {
            String taskId = new String(message.getBody());

            log.info("Receiver Got message {}", taskId);
            int sleep = new Random().nextInt(5000) + 100;
            Thread.sleep(sleep);
            log.info("Receiver completed task {} in: {}", taskId, sleep);

            return Mono.just(taskId + "::" + sleep);
        } catch (InterruptedException ex) {

        }
        return null;
    }


    public void run(int count) {
        ConsumeOptions options = new ConsumeOptions();
        options.qos(1);
        Flux<Delivery> messages = receiver.consumeAutoAck(QUEUE, options);
        messages.delaySubscription(sender.declareQueue(QueueSpecification.queue(QUEUE)))
                .groupBy( (message) -> {
                    int taskId = Integer.parseInt(new String(message.getBody()));
                    return taskId > 100 ? "groupB" : "groupA";
                })
                .map(g -> g.parallel().runOn(Schedulers.newElastic("groupByPool")))
                .subscribe( (stream) -> {
                    stream.subscribe(this::doWork);
                });


        Flux<OutboundMessageResult> dataStream = sender.sendWithPublishConfirms(Flux.range(1, count)
                .map(i -> new OutboundMessage("", QUEUE, (i.toString()).getBytes())));

        sender.declareQueue(QueueSpecification.queue(QUEUE))
                .thenMany(dataStream)
                .doOnError(e -> System.out.println("Send failed" + e))
                .subscribe(m -> {
                    if (m != null) {
                        log.info("Successfully sent message");
                    }
                });

        try {
            Thread.sleep(20000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }

    public static void main(String[] args) throws Exception {
        int count = 200;
        FluxWithRabbitMQDemo sender = new FluxWithRabbitMQDemo();
        sender.run(count);
    }
}