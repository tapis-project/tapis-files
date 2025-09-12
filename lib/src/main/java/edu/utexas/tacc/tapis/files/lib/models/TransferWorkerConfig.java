package edu.utexas.tacc.tapis.files.lib.models;

import org.apache.commons.lang3.StringUtils;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

public class TransferWorkerConfig {
    public enum TransferType {
        TRANSFER_TYPE_PARENT,
        TRANSFER_TYPE_CHILD,
        TRANSFER_TYPE_ARCHIVE
    }

    private Set<TransferType> acceptedTransferTypes = new HashSet<>();
    private Set<String> acceptedTenants = new HashSet<>();

    public TransferWorkerConfig(Collection<TransferType> acceptedTransferTypes/*, Collection<String> acceptedTenants*/) {
        this.acceptedTransferTypes.addAll(acceptedTransferTypes);

        // tenants:
        if(acceptedTenants != null) {
            this.acceptedTenants.addAll(
                    acceptedTenants.stream()
                            // filter out null/blanks
                            .filter(tenantName -> !StringUtils.isBlank(tenantName))
                            // map to lower case value
                            .map(tenantName -> tenantName.toLowerCase())
                            // collect into a set
                            .collect(Collectors.toSet())
            );
        }
    }

    public Set<String> getAcceptedTenants() {
        return acceptedTenants;
    }

    public void setAcceptedTenants(Set<String> acceptedTenants) {
        this.acceptedTenants = acceptedTenants;
    }

    public Set<TransferType> getAcceptedTransferTypes() {
        return acceptedTransferTypes;
    }

    public void setAcceptedTransferTypes(Set<TransferType> acceptedTransferTypes) {
        this.acceptedTransferTypes = acceptedTransferTypes;
    }
}
