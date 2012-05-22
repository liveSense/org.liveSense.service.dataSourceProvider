package org.liveSense.service.DataSourceProvider;

import java.sql.Connection;
import java.sql.SQLException;

import javax.sql.DataSource;

import org.apache.commons.dbcp.BasicDataSource;
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

	public Connection getConnection(String userName, String password)
			throws SQLException;

	public DataSource getDataSource();

	public QueryRunner getQueryRunner();

	public BasicDataSource getDs();

	public String getDataSourceName();

	public String getDriverClassName();

	public String getUrl();

	public String getCaption();

	public String getUsername();

	public String getPassword();

	public String getConnectionProperties();

	public Boolean getDefaultAutoCommit();

	public Boolean getDefaultReadOnly();

	public Integer getDefaultTransactionIsolation();

	public Integer getInitialSize();

	public Integer getMaxActive();

	public Integer getMaxIdle();

	public Integer getMaxWait();

	public String getValidationQuery();
}
