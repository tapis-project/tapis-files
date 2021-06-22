package edu.utexas.tacc.tapis.files.lib.services;


import org.jvnet.hk2.annotations.Service;

import javax.inject.Inject;

@Service
public class ChildTaskTransferService {

    private TransfersService transfersService;

    @Inject
    public ChildTaskTransferService(TransfersService transfersService) {
        this.transfersService = transfersService;
    }



}
