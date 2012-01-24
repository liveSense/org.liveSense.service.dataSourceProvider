package org.liveSense.service.DataSourceProvider;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import javax.sql.DataSource;

import org.apache.felix.scr.annotations.Component;
import org.apache.felix.scr.annotations.Reference;
import org.apache.felix.scr.annotations.ReferenceCardinality;
import org.apache.felix.scr.annotations.ReferencePolicy;
import org.apache.felix.scr.annotations.ReferenceStrategy;
import org.apache.felix.scr.annotations.References;
import org.apache.felix.scr.annotations.Service;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@inherited}
 *
 */
@Component(label="DataSourceStoreService",
		metatype=false,
		description="liveSense Data Source Store Service")
@References(
		value={
				@Reference(name="dataSources", cardinality=ReferenceCardinality.OPTIONAL_MULTIPLE,policy=ReferencePolicy.DYNAMIC,strategy=ReferenceStrategy.EVENT,bind="bind",unbind="unbind",referenceInterface=DataSourceProvider.class)
		})		
@Service(value=DataSourceStoreService.class)
public class DataSourceStoreServiceImpl implements DataSourceStoreService {

	Logger log = LoggerFactory.getLogger(DataSourceStoreServiceImpl.class);
	private Map<String,DataSourceProvider> dataSourceProviders = Collections.synchronizedMap(new HashMap<String, DataSourceProvider>());
	
	public DataSource getDataSource(String name) {
		DataSource ret = null;
		synchronized (dataSourceProviders) {ret = dataSourceProviders.get(name).getDataSource();};
		return ret;
	}

	public DataSourceProvider getDataSourceProvider(String name) {
		DataSourceProvider ret = null;
		synchronized (dataSourceProviders) {ret = dataSourceProviders.get(name);};
		return ret;
	}

	public void bind(DataSourceProvider dataSourceProvider) {
		log.info("Binding dataSource - "+dataSourceProvider.getName());
		dataSourceProviders.put(dataSourceProvider.getName(), dataSourceProvider);
	}

	public void unbind(DataSourceProvider dataSourceProvider) {
		log.info("Unbinding dataSource - "+dataSourceProvider.getName());
		dataSourceProviders.remove(dataSourceProvider.getName());
	}

}
