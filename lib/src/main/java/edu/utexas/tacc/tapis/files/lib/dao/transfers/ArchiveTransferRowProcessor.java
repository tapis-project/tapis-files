package edu.utexas.tacc.tapis.files.lib.dao.transfers;

import edu.utexas.tacc.tapis.files.lib.models.ArchiveTransfer;
import edu.utexas.tacc.tapis.files.lib.models.ArchiveTransferStatus;
import org.apache.commons.dbutils.BasicRowProcessor;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

public class ArchiveTransferRowProcessor extends BasicRowProcessor {
    @Override
    public ArchiveTransfer toBean(ResultSet resultSet, Class type) throws SQLException {
        ArchiveTransfer archiveTransfer = new ArchiveTransfer();
        archiveTransfer.setId(resultSet.getInt("id"));
        archiveTransfer.setUsername(resultSet.getString("username"));
        archiveTransfer.setTenantId(resultSet.getString("tenant_id"));
        archiveTransfer.setSourceBaseUrl(resultSet.getString("source_base_url"));
        archiveTransfer.setDestinationBaseUrl(resultSet.getString("destination_base_url"));
        archiveTransfer.setArchiveType(resultSet.getString("archive_type"));
        archiveTransfer.setCreated(resultSet.getTimestamp("created").toInstant());
        Timestamp nextRetryTimestamp = resultSet.getTimestamp("next_retry");
        if(nextRetryTimestamp != null) {
            archiveTransfer.setNextRetry(nextRetryTimestamp.toInstant());
        }
        archiveTransfer.setRetriesRemaining(resultSet.getInt("retries_remaining"));
        archiveTransfer.setUuid(resultSet.getObject("uuid", UUID.class));
        archiveTransfer.setStatus(ArchiveTransferStatus.valueOf(resultSet.getString("status")));
        archiveTransfer.setSrcSharedCtxGrantor(resultSet.getString("src_shared_ctx"));
        archiveTransfer.setDestSharedCtxGrantor(resultSet.getString("dst_shared_ctx"));
        archiveTransfer.setArchiveBytesRead(resultSet.getLong("archive_bytes_read"));
        archiveTransfer.setFileBytesRead(resultSet.getLong("file_bytes_read"));
        archiveTransfer.setErrorMessage(resultSet.getString("error_message"));
        archiveTransfer.setAssignedTo(resultSet.getObject("assigned_to", UUID.class));
        Optional.ofNullable(resultSet.getTimestamp("start_time")).ifPresent(ts -> archiveTransfer.setStartTime(ts.toInstant()));
        Optional.ofNullable(resultSet.getTimestamp("end_time")).ifPresent(ts -> archiveTransfer.setEndTime(ts.toInstant()));
        return archiveTransfer;
    }

    @Override
    public List<ArchiveTransfer> toBeanList(ResultSet resultSet, Class type) throws SQLException {
        List<ArchiveTransfer> list = new ArrayList<>();
        while (resultSet.next()) {
            list.add(toBean(resultSet, type));
        }
        return list;
    }
}
