package edu.utexas.tacc.tapis.files.lib.transfers;

import edu.utexas.tacc.tapis.files.lib.clients.IRemoteDataClient;
import edu.utexas.tacc.tapis.files.lib.clients.IRemoteDataClientFactory;
import edu.utexas.tacc.tapis.files.lib.models.FileInfo;
import edu.utexas.tacc.tapis.files.lib.models.TransferTask;

import javax.inject.Inject;
import java.util.List;

public class ParentTaskWorker {

    private TransferTask parentTask;

    private IRemoteDataClientFactory remoteDataClientFactory;

    public ParentTaskWorker(TransferTask task) {
        this.parentTask = task;
    }

    public TransferTask getParentTask() {
        return parentTask;
    }

    public List<FileInfo> listAll() {

        return null;
    }

}
