package edu.utexas.tacc.tapis.files.lib.dao.transfers;

import edu.utexas.tacc.tapis.files.lib.models.TransferTaskParent;
import org.apache.commons.dbutils.BasicRowProcessor;
import org.apache.commons.lang3.StringUtils;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

// TODO: There should be some way to not duplicate this code...
class TransferTaskParentRowProcessor extends BasicRowProcessor {
    @Override
    public TransferTaskParent toBean(ResultSet rs, Class type) throws SQLException {
        TransferTaskParent task = new TransferTaskParent();
        task.setId(rs.getInt("id"));
        task.setTaskId(rs.getInt("task_id"));
        task.setUsername(rs.getString("username"));
        task.setTenantId(rs.getString("tenant_id"));
        task.setSourceURI(rs.getString("source_uri"));
        task.setDestinationURI(rs.getString("destination_uri"));
        task.setCreated(rs.getTimestamp("created").toInstant());
        task.setUuid(UUID.fromString(rs.getString("uuid")));
        task.setStatus(rs.getString("status"));
        task.setOptional(rs.getBoolean("optional"));
        task.setSrcSharedCtxGrantor(rs.getString("src_shared_ctx"));
        task.setDestSharedCtxGrantor(rs.getString("dst_shared_ctx"));
        task.setTag(rs.getString("tag"));
        task.setTotalBytes(rs.getLong("total_bytes"));
        task.setBytesTransferred(rs.getLong("bytes_transferred"));
        task.setErrorMessage(rs.getString("error_message"));
        task.setFinalMessage(rs.getString("final_message"));
        String transferTypeString = rs.getString("transfer_type");
        if (!StringUtils.isBlank(transferTypeString)) {
            task.setTransferType(TransferTaskParent.TransferType.valueOf(transferTypeString));
        }
        Optional.ofNullable(rs.getTimestamp("start_time")).ifPresent(ts -> task.setStartTime(ts.toInstant()));
        Optional.ofNullable(rs.getTimestamp("end_time")).ifPresent(ts -> task.setEndTime(ts.toInstant()));
        return task;
    }

    @Override
    public List<TransferTaskParent> toBeanList(ResultSet rs, Class type) throws SQLException {
        List<TransferTaskParent> list = new ArrayList<>();
        while (rs.next()) {
            list.add(toBean(rs, type));
        }
        return list;
    }
}
