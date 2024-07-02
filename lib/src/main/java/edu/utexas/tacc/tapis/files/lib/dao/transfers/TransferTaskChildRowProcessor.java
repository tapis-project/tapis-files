package edu.utexas.tacc.tapis.files.lib.dao.transfers;

import edu.utexas.tacc.tapis.files.lib.models.TransferTaskChild;
import org.apache.commons.dbutils.BasicRowProcessor;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

class TransferTaskChildRowProcessor extends BasicRowProcessor {

    @Override
    public TransferTaskChild toBean(ResultSet rs, Class type) throws SQLException {
        TransferTaskChild task = new TransferTaskChild();
        task.setId(rs.getInt("id"));
        task.setTaskId(rs.getInt("task_id"));
        task.setParentTaskId(rs.getInt("parent_task_id"));
        task.setUsername(rs.getString("username"));
        task.setTenantId(rs.getString("tenant_id"));
        task.setSourceURI(rs.getString("source_uri"));
        task.setDestinationURI(rs.getString("destination_uri"));
        task.setCreated(rs.getTimestamp("created").toInstant());
        task.setRetries(rs.getInt("retries"));
        task.setUuid(UUID.fromString(rs.getString("uuid")));
        task.setStatus(rs.getString("status"));
        task.setDir(rs.getBoolean("is_dir"));
        task.setTotalBytes(rs.getLong("total_bytes"));
        task.setBytesTransferred(rs.getLong("bytes_transferred"));
        task.setErrorMessage(rs.getString("error_message"));
        task.setExternalTaskId(rs.getString("external_task_id"));
        Optional.ofNullable(rs.getTimestamp("start_time")).ifPresent(ts -> task.setStartTime(ts.toInstant()));
        Optional.ofNullable(rs.getTimestamp("end_time")).ifPresent(ts -> task.setEndTime(ts.toInstant()));
        return task;
    }

    @Override
    public List<TransferTaskChild> toBeanList(ResultSet rs, Class type) throws SQLException {
        List<TransferTaskChild> list = new ArrayList<>();
        while (rs.next()) {
            list.add(toBean(rs, type));
        }
        return list;
    }
}
