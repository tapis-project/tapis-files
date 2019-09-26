package edu.utexas.tacc.tapis.files.lib.dao.sharing;

import edu.utexas.tacc.tapis.files.lib.exceptions.DAOException;
import edu.utexas.tacc.tapis.files.lib.models.SharedFileObject;

import java.util.UUID;

public interface IFileSharingDAO {

  SharedFileObject getShared(UUID shareUUID) throws DAOException;
  SharedFileObject createShare(SharedFileObject task) throws DAOException;
}
