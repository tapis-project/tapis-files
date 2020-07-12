package edu.utexas.tacc.tapis.files.lib.models;

import java.time.Instant;
import java.util.UUID;

public interface ITransferTask {

    UUID getUuid();

    void setId(int id);

    int getId();

    Long getTotalBytes();

    void setTotalBytes(Long totalBytes);

    Long getBytesTransferred();

    void setBytesTransferred(Long bytesTransferred);

    String getTenantId();

    void setTenantId(String tenantId);

    String getUsername();

    void setUsername(String username);

    String getSourceSystemId();

    void setSourceSystemId(String sourceSystemId);

    String getSourcePath();

    void setSourcePath(String sourcePath);

    String getDestinationSystemId();

    void setDestinationSystemId(String destinationSystemId);

    String getDestinationPath();

    void setDestinationPath(String destinationPath);

    void setUuid(UUID uuid);

    Instant getCreated();

    void setCreated(Instant created);

    void setCreated(String created);

    String getStatus();

    void setStatus(String status) throws IllegalArgumentException;
}
