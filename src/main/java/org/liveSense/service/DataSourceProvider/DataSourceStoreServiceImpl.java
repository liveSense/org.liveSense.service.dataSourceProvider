package org.liveSense.service.DataSourceProvider;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.sql.DataSource;
import org.apache.felix.scr.annotations.Component;
import org.apache.felix.scr.annotations.Service;


@Component(label="DataSourceStoreService",
		metatype=false,
		description="liveSense Data Source Store Service")
		
@Service(value=DataSourceStoreService.class)
public class DataSourceStoreServiceImpl implements DataSourceStoreService {

	private Map<String,DataSource> dataSources = Collections.synchronizedMap(new HashMap<String, DataSource>());
	
	public DataSource getDataSource(String name) {
		DataSource ret = null;
		synchronized (dataSources) {ret = dataSources.get(name);};
		return ret;
	}

	public void putDataSource(String name, DataSource dataSource) {
		synchronized (dataSources) {dataSources.put(name, dataSource);};
	}

	public void removeDataSource(String name) {
		synchronized (dataSources) {dataSources.remove(name);}
	}

	public void removeDataSource(DataSource dataSource) {
		String removableKey = null;
		synchronized (dataSources) {
			for (String dsName : dataSources.keySet()) {
				if (dataSources.get(dsName).equals(dataSource)) {
					removableKey = dsName;
				}
			}
		}
		if (removableKey != null) {
			removeDataSource(removableKey);
		}
	}

	public List<String> getDataSourceNames() {
		List<String> ret = new ArrayList<String>();
		synchronized (dataSources) {
			for (String dsName : dataSources.keySet()) {
				ret.add(dsName);
			}
		}
		return ret;
	}

}
