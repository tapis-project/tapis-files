package edu.utexas.tacc.tapis.files.lib.transfers;

import edu.utexas.tacc.tapis.files.lib.models.TransferWorkerConfig;
import org.apache.commons.collections.CollectionUtils;

import java.time.Instant;
import java.util.Set;
import java.util.UUID;

public class TransferWorker {

    record AssignmentParams(TransferWorkerConfig.TransferType transferType, String tenantId) {};

    private UUID uuid;
    private Instant lastUpdated;

    private TransferWorkerConfig transferWorkerConfig;

    public TransferWorker() {
        this.uuid = uuid;
    }

    public TransferWorker(UUID uuid, Instant lastUpdated, TransferWorkerConfig transferWorkerConfig) {
        this.uuid = uuid;
        this.lastUpdated = lastUpdated;
        this.transferWorkerConfig = transferWorkerConfig;
    }

    public UUID getUuid() {
        return uuid;
    }

    public Instant getLastUpdated() {
        return lastUpdated;
    }

    public TransferWorkerConfig getTransferWorkerConfig() {
        return transferWorkerConfig;
    }

    /**
     * Looks to see if the assingmentParams allow for assignment.  Note that if any of the
     * params in assignment params are null, they will not be checked.
     * @param assignmentParams
     * @return
     */
    public boolean canAssignTask(AssignmentParams assignmentParams) {
        // return false at any point if it's excluded - if we get to the end we will accept it.
        if(transferWorkerConfig == null) {
            return false;
        }

        // look at the Type - skip if the param was left null
        if(assignmentParams.transferType != null) {
            // if there are no accepted types, we accept anything
            Set<TransferWorkerConfig.TransferType> acceptedTypes = transferWorkerConfig.getAcceptedTransferTypes();
            if (!CollectionUtils.isEmpty(acceptedTypes)) {
                // we have accepted types, so if this in to one of them we will not accept it.
                if (!acceptedTypes.contains(assignmentParams.transferType)) {
                    return false;
                }
            }
        }

        // look at the TenantId - skip if the param was left null
        // if there are no accepted tenants, we accept anything
        if(assignmentParams.tenantId != null) {
            Set<String> acceptedTenants = transferWorkerConfig.getAcceptedTenants();
            if (!CollectionUtils.isEmpty(acceptedTenants)) {
                // we have accepted types, so if this in to one of them we will not accept it.
                if (!acceptedTenants.contains(assignmentParams.tenantId)) {
                    return false;
                }
            }
        }

        return true;
    }
}
