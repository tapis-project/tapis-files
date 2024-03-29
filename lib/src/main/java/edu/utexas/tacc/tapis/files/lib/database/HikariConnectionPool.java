package edu.utexas.tacc.tapis.files.lib.database;


import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import edu.utexas.tacc.tapis.files.lib.config.IRuntimeConfig;
import edu.utexas.tacc.tapis.files.lib.config.RuntimeSettings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;

public class HikariConnectionPool {
    private static HikariConfig config = new HikariConfig();
    private static HikariDataSource ds;
    private static IRuntimeConfig conf = RuntimeSettings.get();
    private static final Logger log = LoggerFactory.getLogger(HikariConnectionPool.class);

    static {
        String dbUrl = String.format("jdbc:postgresql://%s:%s/%s",
            conf.getDbHost(),
            conf.getDbPort(),
            conf.getDbName()
        );
        config.setJdbcUrl(dbUrl);
        config.setUsername(conf.getDbUsername());
        config.setPassword(conf.getDbPassword());
        config.addDataSourceProperty("cachePrepStmts", "true");
        config.addDataSourceProperty("prepStmtCacheSize", "250");
        config.addDataSourceProperty("prepStmtCacheSqlLimit", "2048");
        config.setMaximumPoolSize(conf.getDbConnectionPoolSize());
        config.setMinimumIdle(conf.getDbConnectionPoolCoreSize());
        ds = new HikariDataSource(config);
    }

    private HikariConnectionPool() {
    }

    public static Connection getConnection() {
        try {
            Connection connection = ds.getConnection();
            return connection;
        } catch (SQLException ex) {
            throw new RuntimeException("Error connecting to database");
        }
    }

    public static DataSource getDataSource() { return ds; }
}
