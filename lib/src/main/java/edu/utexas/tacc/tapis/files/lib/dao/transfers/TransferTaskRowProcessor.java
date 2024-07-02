package edu.utexas.tacc.tapis.files.lib.dao.transfers;

import edu.utexas.tacc.tapis.files.lib.models.TransferTask;
import org.apache.commons.dbutils.BasicRowProcessor;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

// TODO: There should be some way to not duplicate this code...
class TransferTaskRowProcessor extends BasicRowProcessor {

    @Override
    public TransferTask toBean(ResultSet rs, Class type) throws SQLException {
        TransferTask task = new TransferTask();
        task.setId(rs.getInt("id"));
        task.setTenantId(rs.getString("tenant_id"));
        task.setUsername(rs.getString("username"));
        task.setCreated(rs.getTimestamp("created").toInstant());
        task.setUuid(UUID.fromString(rs.getString("uuid")));
        task.setStatus(rs.getString("status"));
        task.setTag(rs.getString("tag"));
        task.setErrorMessage(rs.getString("error_message"));
        Optional.ofNullable(rs.getTimestamp("start_time")).ifPresent(ts -> task.setStartTime(ts.toInstant()));
        Optional.ofNullable(rs.getTimestamp("end_time")).ifPresent(ts -> task.setEndTime(ts.toInstant()));
        return task;
    }

    @Override
    public List<TransferTask> toBeanList(ResultSet rs, Class type) throws SQLException {
        List<TransferTask> list = new ArrayList<>();
        while (rs.next()) {
            list.add(toBean(rs, type));
        }
        return list;
    }
}
