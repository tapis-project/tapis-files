package edu.utexas.tacc.tapis.files.lib.transfers;

import edu.utexas.tacc.tapis.files.lib.models.TransferControlAction;
import reactor.core.publisher.Sinks;

public class ControlMessageEmitter {

    private final Sinks.Many<TransferControlAction> sink = Sinks.many().multicast().onBackpressureBuffer();


}
