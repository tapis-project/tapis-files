package edu.utexas.tacc.tapis.files.lib.dao.transfers;

import edu.utexas.tacc.tapis.files.lib.exceptions.DAOException;
import edu.utexas.tacc.tapis.files.lib.models.TransferTask;

import java.util.UUID;

public interface IFileTransferDAO {

  TransferTask getTransferTask(UUID taskUuid) throws DAOException;
  TransferTask createTransferTask(TransferTask task) throws DAOException;
}
