package edu.utexas.tacc.tapis.files.lib.transfers;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.ConnectionFactory;
import edu.utexas.tacc.tapis.files.lib.models.TransferTaskChild;
import edu.utexas.tacc.tapis.files.lib.models.TransferTaskStatus;
import edu.utexas.tacc.tapis.files.lib.services.TransfersService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.rabbitmq.*;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicInteger;

public class Emitter {
    private Logger log = LoggerFactory.getLogger(Emitter.class);
    private static final String QUEUE = "tapis.files.transfers.child";

    public void run(int count) {
        final Receiver receiver;
        final Sender sender;
        final ObjectMapper mapper = new ObjectMapper();
        ConnectionFactory connectionFactory = new ConnectionFactory();
        connectionFactory.setUsername("dev");
        connectionFactory.setPassword("dev");
        connectionFactory.setVirtualHost("dev");
        connectionFactory.useNio();
        ReceiverOptions receiverOptions = new ReceiverOptions()
          .connectionFactory(connectionFactory)
          .connectionSubscriptionScheduler(Schedulers.newElastic("child-receiver"));
        SenderOptions senderOptions = new SenderOptions()
          .connectionFactory(connectionFactory)
          .resourceManagementScheduler(Schedulers.newElastic("child-sender"));
        receiver = RabbitFlux.createReceiver(receiverOptions);
        sender = RabbitFlux.createSender(senderOptions);


        AtomicInteger counter = new AtomicInteger(0);
        Flux<OutboundMessage> data = Flux.range(1, count)
          .window(10)
          .flatMap( g -> {
                counter.getAndIncrement();

                return g.map((i) -> {
                    int parentTaskId = counter.get();
                    TransferTaskChild childMessage = new TransferTaskChild();
                    childMessage.setId(i);
                    childMessage.setTenantId("tenantA");
                    childMessage.setParentTaskId(g);
                    childMessage.setStatus(TransferTaskStatus.ACCEPTED);
                    childMessage.setDestinationPath("/a/b/c");
                    childMessage.setSourcePath("/d/e/f");
                    childMessage.setSourceSystemId("systemA");
                    childMessage.setDestinationSystemId("systemB");
                    try {
                        String m = mapper.writeValueAsString(childMessage);
                        OutboundMessage message = new OutboundMessage("", QUEUE, m.getBytes(StandardCharsets.UTF_8));
                        return Flux.just(message);
                    } catch (Exception e) {

                    }
                });
            });

        sender.send(data).subscribe();
    }

    public static void main(String[] args) {
        Emitter e = new Emitter();
        e.run(2000);
    }

}
