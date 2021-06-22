package edu.utexas.tacc.tapis.files.lib.services;

import org.jvnet.hk2.annotations.Service;

import javax.inject.Inject;

@Service
public class FileTransferAgent {

    private TransfersService transfersService;

    @Inject
    public FileTransferAgent(TransfersService transfersService) {
        this.transfersService = transfersService;
    }




}
