package edu.utexas.tacc.tapis.files.lib.dao.transfers;

import edu.utexas.tacc.tapis.files.lib.exceptions.DAOException;
import edu.utexas.tacc.tapis.files.lib.models.TransferTask;
import edu.utexas.tacc.tapis.files.lib.models.TransferTaskChild;

import javax.validation.constraints.NotNull;
import java.util.List;
import java.util.UUID;

public interface IFileTransferDAO {

  TransferTask getTransferTaskById(long taskId) throws DAOException;
  TransferTask createTransferTask(TransferTask task) throws DAOException;
  List<TransferTaskChild> getAllChildren(@NotNull TransferTask task) throws DAOException;
  TransferTaskChild getChildTask(@NotNull TransferTaskChild task) throws DAOException;
}
