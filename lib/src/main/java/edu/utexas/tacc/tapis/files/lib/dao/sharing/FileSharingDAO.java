package edu.utexas.tacc.tapis.files.lib.dao.sharing;

import edu.utexas.tacc.tapis.files.lib.database.ConnectionPool;
import edu.utexas.tacc.tapis.files.lib.exceptions.DAOException;
import edu.utexas.tacc.tapis.files.lib.models.SharedFileObject;
import edu.utexas.tacc.tapis.files.lib.models.TransferTask;
import org.apache.commons.dbutils.*;
import org.apache.commons.dbutils.handlers.BeanHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.validation.constraints.NotNull;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.UUID;

public class FileSharingDAO implements IFileSharingDAO {

  private Logger log = LoggerFactory.getLogger(FileSharingDAO.class);
  private RowProcessor rowProcessor = new BasicRowProcessor(new GenerousBeanProcessor());


  @Override
  public SharedFileObject getShared(UUID shareUUID) throws DAOException {
    return null;
  }

  @Override
  public SharedFileObject createShare(SharedFileObject task) throws DAOException {
    return null;
  }
}
