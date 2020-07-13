package edu.utexas.tacc.tapis.files.lib.transfers;

import com.fasterxml.jackson.databind.ObjectMapper;
import edu.utexas.tacc.tapis.files.lib.clients.IRemoteDataClientFactory;
import edu.utexas.tacc.tapis.files.lib.clients.RemoteDataClientFactory;
import edu.utexas.tacc.tapis.files.lib.json.TapisObjectMapper;
import edu.utexas.tacc.tapis.files.lib.models.TransferTask;
import edu.utexas.tacc.tapis.files.lib.services.TransfersService;
import edu.utexas.tacc.tapis.files.lib.utils.SystemsClientFactory;
import org.jvnet.hk2.annotations.Service;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.statefulj.fsm.FSM;
import org.statefulj.fsm.TooBusyException;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import javax.inject.Inject;
import javax.inject.Named;
import javax.validation.constraints.NotNull;
import java.io.IOException;

/**
 *
 */

@Service
@Named
public class ParentTaskReceiver implements Runnable {


    private static final Logger log = LoggerFactory.getLogger(ParentTaskReceiver.class);
    private static final int MAX_RETRIES = 5;
    private static final ObjectMapper mapper = TapisObjectMapper.getMapper();

    private final TransfersService transfersService;

    @Inject
    public ParentTaskReceiver(@NotNull TransfersService transfersService) {

        this.transfersService = transfersService;
    }

    public void run() {
        transfersService.streamParentMessages()
            .groupBy((message) -> {
                try {
                    return Mono.just(mapper.readValue(message.getBody(), TransferTask.class).getTenantId());
                } catch (IOException ex) {
                    log.error("invalid message", ex);
                    return Mono.error(ex);
                }
            }).map(Flux::parallel)
            .subscribe(group -> {
                group.runOn(Schedulers.newBoundedElastic(8, 1, "pool"))
                    //flatMap allows for an empty sequence if the processing step failed
                    .flatMap((taskMessage) -> {
                        try {
                            TransferTask task = mapper.readValue(taskMessage.getBody(), TransferTask.class);
                            return transfersService.doStepOne(task);
                        } catch (IOException ex) {
                            return Mono.error(ex);
                        }
                    })
                    .flatMap((task)->{
                            return Mono.just(task);
                    }).subscribe();
            });
    }


    public static void main(String[] args) throws Exception {

        Flux.range(1, 10)
            .map((i) -> {
                if (i == 4) {
                    return Flux.error(new IOException());
                }
                return i * 2;
            })
            .onErrorContinue((ex, o) -> {
                log.info(ex.getMessage(), o.toString());
            })
            .map((i) -> {
                log.info(i.toString());
                return i;
            })
            .subscribe();


    }

}