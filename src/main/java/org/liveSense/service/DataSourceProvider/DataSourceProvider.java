package org.liveSense.service.DataSourceProvider;

import java.sql.Connection;
import java.sql.SQLException;

import javax.sql.DataSource;

public interface DataSourceProvider {
	 public Connection getConnection(String dataSource) throws NoDataSourceFound, SQLException;
	 public Connection getConnection(String dataSource, String userName, String password) throws NoDataSourceFound, SQLException;
	 public DataSource getDataSource(String dataSource) throws NoDataSourceFound;   

}
