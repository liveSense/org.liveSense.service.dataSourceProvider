package org.liveSense.service.DataSourceProvider;

import java.sql.Connection;
import java.sql.SQLException;

import javax.sql.DataSource;

import org.apache.commons.dbutils.QueryRunner;

/**
 * Simple OSGi datasource provider based on Apache DBCP.
 *
 * @author robson
 *
 */
public interface DataSourceProvider {
	 public String getName();
	 public Connection getConnection() throws SQLException;
	 public Connection getConnection(String userName, String password) throws SQLException;
	 public DataSource getDataSource();   
	 public QueryRunner getQueryRunner();
}
