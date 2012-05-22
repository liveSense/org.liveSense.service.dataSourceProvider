package org.liveSense.service.DataSourceProvider;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

import javax.sql.DataSource;

import org.apache.commons.lang.StringUtils;
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
	List<DataSourceProvider> dataSourceProviders = new CopyOnWriteArrayList<DataSourceProvider>();
	
	public DataSource getDataSource(String name) {
		DataSourceProvider prov = getDataSourceProvider(name);
		if (prov != null) return prov.getDataSource();
		return null;
	}

	public DataSourceProvider getDataSourceProvider(String name) {
		DataSourceProvider ret = null;
		for (DataSourceProvider ds : dataSourceProviders) {
			if (StringUtils.isNotEmpty(name) && name.equals(ds.getName())) {
				return ds;
			}
		}
		return null;
	}

	public void bind(DataSourceProvider dataSourceProvider) {
		log.info("Binding dataSource - "+dataSourceProvider.getName());
		dataSourceProviders.add(dataSourceProvider);
	}

	public void unbind(DataSourceProvider dataSourceProvider) {
		log.info("Unbinding dataSource - "+dataSourceProvider.getName());
		dataSourceProviders.remove(dataSourceProvider);
	}

	public List<DataSourceProvider> getAllDataSourceProvider() {
		return new ArrayList<DataSourceProvider>(dataSourceProviders);
	}

}
