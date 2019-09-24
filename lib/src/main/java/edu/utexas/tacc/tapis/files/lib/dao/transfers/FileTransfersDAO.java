package edu.utexas.tacc.tapis.files.lib.dao.transfers;

import edu.utexas.tacc.tapis.files.lib.database.ConnectionPool;
import edu.utexas.tacc.tapis.files.lib.exceptions.DAOException;
import edu.utexas.tacc.tapis.files.lib.models.TransferTask;
import org.apache.commons.dbutils.DbUtils;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.handlers.ScalarHandler;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.UUID;
import org.slf4j.Logger;

public class FileTransfersDAO implements IFileTransferDAO {

  private Logger log = LoggerFactory.getLogger(FileTransfersDAO.class);

  @Override
  public TransferTask getTransferTask(UUID taskUUID) throws DAOException {
    Connection connection = ConnectionPool.getConnection();
    try {
      ScalarHandler<TransferTask> scalarHandler = new ScalarHandler<>();
      String query = "SELECT * FROM transfer_tasks where uuid= ?";
      QueryRunner runner = new QueryRunner();
      TransferTask task = runner.query(connection, query, scalarHandler, taskUUID);
      return task;
    } catch (SQLException ex) {
      throw new DAOException(ex.getErrorCode());
    } finally {
      DbUtils.closeQuietly(connection);
    }
  }

  @Override
  public TransferTask createTransferTask() {
    return null;
  }
}
