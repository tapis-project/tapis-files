package edu.utexas.tacc.tapis.files.lib.dao.transfers;

import edu.utexas.tacc.tapis.files.lib.models.TransferTaskSummary;
import org.apache.commons.dbutils.BasicRowProcessor;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

class TransferTaskSummaryRowProcessor extends BasicRowProcessor {
    @Override
    public TransferTaskSummary toBean(ResultSet rs, Class type) throws SQLException {
        TransferTaskSummary summary = new TransferTaskSummary();
        summary.setCompleteTransfers(rs.getInt("complete"));
        summary.setTotalTransfers(rs.getInt("total_transfers"));
        summary.setEstimatedTotalBytes(rs.getLong("total_bytes"));
        summary.setTotalBytesTransferred(rs.getLong("total_bytes_transferred"));
        return summary;
    }

    @Override
    public List<TransferTaskSummary> toBeanList(ResultSet rs, Class type) throws SQLException {
        List<TransferTaskSummary> list = new ArrayList<>();
        while (rs.next()) {
            list.add(toBean(rs, type));
        }
        return list;
    }
}
