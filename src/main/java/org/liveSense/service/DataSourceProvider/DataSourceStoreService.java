package org.liveSense.service.DataSourceProvider;

import java.util.List;

import javax.sql.DataSource;

public interface DataSourceStoreService {
	
	public DataSource getDataSource(String name);
	
	public void putDataSource(String name, DataSource dataSource);
	
	public void removeDataSource(String name);
	
	public void removeDataSource(DataSource daraSource);
	
	public List<String> getDataSourceNames();
}
