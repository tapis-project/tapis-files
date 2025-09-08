package edu.utexas.tacc.tapis.files.lib.dao.transfers;

import edu.utexas.tacc.tapis.files.lib.models.TransferWorkerConfig;
import edu.utexas.tacc.tapis.files.lib.transfers.TransferWorker;
import edu.utexas.tacc.tapis.shared.utils.TapisGsonUtils;
import org.apache.commons.dbutils.BasicRowProcessor;
import org.postgresql.util.PGobject;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class TransferWorkersRowProcessor extends BasicRowProcessor {
    @Override
    public TransferWorker toBean(ResultSet rs, Class type) throws SQLException {
        TransferWorkerConfig transferWorkerConfig = null;
        String uuidString = rs.getString("uuid");
        Timestamp lastUpdated = rs.getTimestamp("last_updated");
        PGobject transferWorkerConfigPGObject = (PGobject) rs.getObject("worker_config");
        if(!transferWorkerConfigPGObject.isNull()) {
            transferWorkerConfig = TapisGsonUtils.getGson().fromJson(transferWorkerConfigPGObject.getValue(), TransferWorkerConfig.class);
        }

        TransferWorker worker = new TransferWorker(UUID.fromString(uuidString), lastUpdated.toInstant(), transferWorkerConfig);
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
