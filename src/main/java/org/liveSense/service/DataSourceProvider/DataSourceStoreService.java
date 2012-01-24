package org.liveSense.service.DataSourceProvider;

import javax.sql.DataSource;

/**
 * OSGi service for managing dataSources. Multiple database and database type is supported.
 * 
 * @author robson
 *
 */
public interface DataSourceStoreService {
	
	public DataSource getDataSource(String name);
	public DataSourceProvider getDataSourceProvider(String name);
}
