package edu.utexas.tacc.tapis.files.lib.transfers;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.ConnectionFactory;
import edu.utexas.tacc.tapis.files.lib.json.TapisObjectMapper;
import edu.utexas.tacc.tapis.files.lib.models.TransferTask;
import edu.utexas.tacc.tapis.files.lib.models.TransferTaskStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.rabbitmq.*;

import java.nio.charset.StandardCharsets;
import java.time.Instant;

public class ParentEmitter {
    private Logger log = LoggerFactory.getLogger(ParentEmitter.class);
    private static final String QUEUE = "tapis.files.transfers.parent";
    private static final ObjectMapper mapper = TapisObjectMapper.getMapper();

    public void run(int count) {
        final Sender sender;
        ConnectionFactory connectionFactory = new ConnectionFactory();
        connectionFactory.setUsername("dev");
        connectionFactory.setPassword("dev");
        connectionFactory.setVirtualHost("dev");
        connectionFactory.useNio();
        SenderOptions senderOptions = new SenderOptions()
          .connectionFactory(connectionFactory)
          .resourceManagementScheduler(Schedulers.newElastic("child-sender"));
        sender = RabbitFlux.createSender(senderOptions);


        Flux<OutboundMessage> messageFlux = Flux.range(1, count)
          .groupBy((i) -> {
              return i / 100;
          })
          .flatMap(group -> group.map(i -> {
              TransferTask task = new TransferTask();
              task.setId(i);
              task.setUsername("user1");
              task.setTenantId("tenant-" + group.key());
              task.setStatus(TransferTaskStatus.ACCEPTED);
              task.setDestinationPath("/a/b/c");
              task.setSourcePath("/d/e/f");
              task.setSourceSystemId("systemA");
              task.setDestinationSystemId("systemB");
              task.setCreated(Instant.now());
              try {
                  String m = mapper.writeValueAsString(task);
                  return new OutboundMessage("", QUEUE, m.getBytes(StandardCharsets.UTF_8));
              } catch (Exception e) {
                  log.info(e.getMessage());
                  return null;
              }

          }));

        sender.send(messageFlux).subscribe();
    }

    public static void main(String[] args) {
        ParentEmitter e = new ParentEmitter();
        e.run(2000);
    }

}
