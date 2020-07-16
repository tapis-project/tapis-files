package edu.utexas.tacc.tapis.files.lib.transfers;

import com.fasterxml.jackson.databind.ObjectMapper;
import edu.utexas.tacc.tapis.files.lib.clients.IRemoteDataClientFactory;
import edu.utexas.tacc.tapis.files.lib.clients.RemoteDataClientFactory;
import edu.utexas.tacc.tapis.files.lib.exceptions.ServiceException;
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
public class ParentTaskReceiver {


    private static final Logger log = LoggerFactory.getLogger(ParentTaskReceiver.class);
    private static final int MAX_RETRIES = 5;
    private static final ObjectMapper mapper = TapisObjectMapper.getMapper();

    private final TransfersService transfersService;

    @Inject
    public ParentTaskReceiver(@NotNull TransfersService transfersService) {
        this.transfersService = transfersService;
    }



}