package org.liveSense.service.DataSourceProvider;

import java.sql.Connection;
import java.sql.SQLException;

import javax.sql.DataSource;

import org.liveSense.api.sql.exceptions.NoDataSourceFound;

/**
 * Simple OSGi datasource provider based on Apache DBCP.
 *
 * @author robson
 *
 */
public interface DataSourceProvider {
	 public Connection getConnection(String dataSource) throws NoDataSourceFound, SQLException;
	 public Connection getConnection(String dataSource, String userName, String password) throws NoDataSourceFound, SQLException;
	 public DataSource getDataSource(String dataSource) throws NoDataSourceFound;   

}
