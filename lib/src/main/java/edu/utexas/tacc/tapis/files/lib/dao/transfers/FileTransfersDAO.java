package edu.utexas.tacc.tapis.files.lib.dao.transfers;

import edu.utexas.tacc.tapis.files.lib.database.HikariConnectionPool;
import edu.utexas.tacc.tapis.files.lib.exceptions.DAOException;
import edu.utexas.tacc.tapis.files.lib.models.TransferTask;
import org.apache.commons.dbutils.*;
import org.apache.commons.dbutils.handlers.BeanHandler;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.UUID;
import org.slf4j.Logger;

import javax.validation.constraints.NotNull;

public class FileTransfersDAO implements IFileTransferDAO {

  private Logger log = LoggerFactory.getLogger(FileTransfersDAO.class);
  private RowProcessor rowProcessor = new BasicRowProcessor(new GenerousBeanProcessor());

  public TransferTask getTransferTask(@NotNull String uuid) throws DAOException {
    UUID taskUUID = UUID.fromString(uuid);
    return getTransferTask(taskUUID);
  }

  public TransferTask getTransferTask(@NotNull  UUID taskUUID) throws DAOException {
    Connection connection = HikariConnectionPool.getConnection();
    try {
      BeanHandler<TransferTask> handler = new BeanHandler<>(TransferTask.class, rowProcessor);
      String query = "SELECT * FROM transfer_tasks where uuid= ?";
      QueryRunner runner = new QueryRunner();
      TransferTask task = runner.query(connection, query, handler, taskUUID);
      return task;
    } catch (SQLException ex) {
      throw new DAOException(ex.getErrorCode());
    } finally {
      DbUtils.closeQuietly(connection);
    }
  }

  public TransferTask createTransferTask(@NotNull TransferTask task) throws DAOException {

    Connection connection = HikariConnectionPool.getConnection();
    try {
      String stmt = "INSERT into transfer_tasks " +
          "(tenant_id, username, uuid, source_system_id, source_path, destination_system_id, destination_path, status)" +
          "values (?, ?, ?, ?, ?, ?, ?, ?)";
      QueryRunner runner = new QueryRunner();
      int insert = runner.update(connection, stmt, task.getTenantId(), task.getUsername(), task.getUuid(),
          task.getSourceSystemId(), task.getSourcePath(), task.getDestinationSystemId(), task.getDestinationPath(), task.getStatus());

      TransferTask insertedTask = getTransferTask(task.getUuid());
      return insertedTask;
    } catch (SQLException ex) {
      throw new DAOException(ex.getErrorCode());
    } finally {
      DbUtils.closeQuietly(connection);
    }
  }
}
