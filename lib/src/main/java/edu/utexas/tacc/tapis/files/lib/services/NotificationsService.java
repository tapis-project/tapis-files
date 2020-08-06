package edu.utexas.tacc.tapis.files.lib.services;


import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Delivery;
import edu.utexas.tacc.tapis.files.lib.exceptions.ServiceException;
import edu.utexas.tacc.tapis.files.lib.json.TapisObjectMapper;
import edu.utexas.tacc.tapis.files.lib.models.FilesNotification;
import edu.utexas.tacc.tapis.files.lib.rabbit.RabbitMQConnection;
import org.jvnet.hk2.annotations.Service;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.rabbitmq.*;

import javax.inject.Inject;
import java.io.IOException;

@Service
public class NotificationsService {

    private static final Logger log = LoggerFactory.getLogger(NotificationsService.class);
    private static final int MAX_RETRIES = 5;
    private final Receiver receiver;
    private final Sender sender;
    private static final String QUEUE_NAME = "tapis.files.notifications";

    private static final ObjectMapper mapper = TapisObjectMapper.getMapper();

    @Inject
    public NotificationsService() {
        ConnectionFactory connectionFactory = RabbitMQConnection.getInstance();
        ReceiverOptions receiverOptions = new ReceiverOptions()
            .connectionFactory(connectionFactory)
            .connectionSubscriptionScheduler(Schedulers.newElastic("receiver"));
        SenderOptions senderOptions = new SenderOptions()
            .connectionFactory(connectionFactory)
            .resourceManagementScheduler(Schedulers.newElastic("sender"));
        receiver = RabbitFlux.createReceiver(receiverOptions);
        sender = RabbitFlux.createSender(senderOptions);
    }

    public void sendNotification(String tenantId, String recipient, String message) throws ServiceException {
        try {
            FilesNotification note = new FilesNotification(tenantId, recipient, message);
            String m = mapper.writeValueAsString(note);
            OutboundMessage outboundMessage = new OutboundMessage("", QUEUE_NAME, m.getBytes());
            sender.send(Mono.just(outboundMessage)).subscribe();
        } catch (IOException ex) {
            log.error(ex.getMessage(), ex);
            throw new ServiceException(ex.getMessage(), ex);
        }
    }

    private Mono<FilesNotification> deserializeNotification(Delivery message) {
        try {
            FilesNotification note = mapper.readValue(message.getBody(), FilesNotification.class);
            return Mono.just(note);
        } catch (IOException ex) {
            log.error("ERROR: Could new deserialize message");
            return Mono.empty();
        }
    }

    public Flux<FilesNotification> streamNotifications() {
        ConsumeOptions options = new ConsumeOptions();
        options.qos(1000);
        Flux<Delivery> childMessageStream = receiver.consumeAutoAck(QUEUE_NAME, options);
        return childMessageStream
            .delaySubscription(sender.declareQueue(QueueSpecification.queue(QUEUE_NAME)))
            .flatMap(this::deserializeNotification);

    }

}
