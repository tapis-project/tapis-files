package edu.utexas.tacc.tapis.files.lib.dao.transfers;

import edu.utexas.tacc.tapis.files.lib.transfers.TransferWorker;
import org.apache.commons.dbutils.BasicRowProcessor;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class TransferWorkersRowProcessor extends BasicRowProcessor {
    @Override
    public TransferWorker toBean(ResultSet rs, Class type) throws SQLException {
        String uuidString = rs.getString("uuid");
        Timestamp lastUpdated = rs.getTimestamp("last_updated");

        TransferWorker worker = new TransferWorker(UUID.fromString(uuidString), lastUpdated.toInstant());
        return worker;
    }

    @Override
    public List<TransferWorker> toBeanList(ResultSet rs, Class type) throws SQLException {
        List<TransferWorker> list = new ArrayList<>();
        while (rs.next()) {
            list.add(toBean(rs, type));
        }
        return list;
    }
}
