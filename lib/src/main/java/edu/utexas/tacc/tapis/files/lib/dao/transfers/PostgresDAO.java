package edu.utexas.tacc.tapis.files.lib.dao.transfers;

import edu.utexas.tacc.tapis.files.lib.exceptions.DAOException;
import edu.utexas.tacc.tapis.files.lib.utils.LibUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSet;
import java.sql.SQLException;

// Used for generic postgres queries - for example getting the version of postgres
public class PostgresDAO {
    Logger log = LoggerFactory.getLogger(PostgresDAO.class);

    public long getPostgresVersion(DAOTransactionContext context) throws DAOException {

        try {
            ResultSet resultSet = context.getConnection().createStatement().executeQuery("SELECT current_setting('server_version_num');");
            resultSet.next();
            long postgresVersion = resultSet.getLong(1);
            log.warn("FILES_TXFR_DAO_POSTGRES_VERSION", postgresVersion);
            return postgresVersion;
        } catch (SQLException ex) {
            throw new DAOException(LibUtils.getMsg("FILES_TXFR_DAO_ERR_GENERAL", "getAcceptedParentTasksForTenantsAndUsers", ex.getMessage()), ex);
        }
    }
}
