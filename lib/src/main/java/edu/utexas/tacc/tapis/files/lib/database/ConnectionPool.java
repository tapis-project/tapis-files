package edu.utexas.tacc.tapis.files.lib.database;


import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import edu.utexas.tacc.tapis.files.lib.config.Settings;

import java.sql.Connection;
import java.sql.SQLException;

public class ConnectionPool {
  private static HikariConfig config = new HikariConfig();
  private static HikariDataSource ds;

  static {
    String dbUrl = String.format("jdbc:postgresql://{}:{}/{}", Settings.get("DB_HOST"), Settings.get("DB_PORT"), Settings.get("DB_NAME"));
    config.setJdbcUrl( dbUrl );
    config.setUsername( Settings.get("DB_USERNAME") );
    config.setPassword( Settings.get("DB_PASSWORD") );
    config.addDataSourceProperty( "cachePrepStmts" , "true" );
    config.addDataSourceProperty( "prepStmtCacheSize" , "250" );
    config.addDataSourceProperty( "prepStmtCacheSqlLimit" , "2048" );
    ds = new HikariDataSource( config );
  }

  private ConnectionPool() {}

  public static Connection getConnection() throws SQLException {
    return ds.getConnection();
  }
}
