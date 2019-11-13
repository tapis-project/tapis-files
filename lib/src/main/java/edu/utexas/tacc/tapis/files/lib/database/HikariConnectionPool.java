package edu.utexas.tacc.tapis.files.lib.database;


import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import edu.utexas.tacc.tapis.files.lib.config.Settings;

import java.sql.Connection;
import java.sql.SQLException;

public class HikariConnectionPool {
  private static HikariConfig config = new HikariConfig();
  private static HikariDataSource ds;
  private static Settings settings=  new Settings();

  static {
    String dbUrl = String.format("jdbc:postgresql://%s:%s/%s", settings.get("DB_HOST"), settings.get("DB_PORT"), settings.get("DB_NAME"));
    config.setJdbcUrl( dbUrl );
    config.setUsername( settings.get("DB_USERNAME") );
    config.setPassword( settings.get("DB_PASSWORD") );
    config.addDataSourceProperty( "cachePrepStmts" , "true" );
    config.addDataSourceProperty( "prepStmtCacheSize" , "250" );
    config.addDataSourceProperty( "prepStmtCacheSqlLimit" , "2048" );
    ds = new HikariDataSource( config );
  }

  private HikariConnectionPool() {}

  public static Connection getConnection() {
    try {
       return ds.getConnection();
    } catch (SQLException ex) {
      throw new RuntimeException("Error connecting to database");
    }
  }
}
