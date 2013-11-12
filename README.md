# [liveSense :: Service :: JDBC DataSource pooler - org.liveSense.service.dataSourceProvider](http://github.com/liveSense/org.liveSense.service.dataSourceProvider)

## Description
Simple DBCP pooled datasource provider.

## OSGi Exported packages
* org.liveSense.api.beanprocessors(1.0.0.SNAPSHOT)
* org.liveSense.api.dao(1.0.0.SNAPSHOT)
* org.liveSense.api.sql(1.0.0.SNAPSHOT)
* org.liveSense.api.sql.exceptions(1.0.0.SNAPSHOT)
* org.liveSense.service.DataSourceProvider(1.0.0.SNAPSHOT)

## OSGi Dependencies
* __System Bundle - org.apache.felix.framework (4.0.3)__
	* javax.sql
	* org.osgi.framework
* __Apache Felix Configuration Admin Service - org.apache.felix.configadmin (1.6.0)__
	* org.osgi.service.cm
* __[liveSense :: Misc :: SQL QueryBuilder - org.liveSense.misc.queryBuilder (2-SNAPSHOT)](http://github.com/liveSense/org.liveSense.misc.queryBuilder)__
	* org.liveSense.misc.queryBuilder
	* org.liveSense.misc.queryBuilder.clauses
	* org.liveSense.misc.queryBuilder.criterias
	* org.liveSense.misc.queryBuilder.domains
	* org.liveSense.misc.queryBuilder.exceptions
	* org.liveSense.misc.queryBuilder.jdbcDriver
	* org.liveSense.misc.queryBuilder.operands
	* org.liveSense.misc.queryBuilder.operators
* __Commons Lang - org.apache.commons.lang (2.6)__
	* org.apache.commons.lang
* __Commons BeanUtils - org.apache.commons.beanutils (1.8.3)__
	* org.apache.commons.beanutils
* __Commons DBCP - org.apache.commons.dbcp (1.4)__
	* org.apache.commons.dbcp
* __Commons DbUtils - org.apache.commons.dbutils (1.3)__
	* org.apache.commons.dbutils
	* org.apache.commons.dbutils.handlers
* __OPS4J Pax Logging - API - org.ops4j.pax.logging.pax-logging-api (1.7.0)__
	* org.knopflerfish.service.log
	* org.ops4j.pax.logging
	* org.osgi.service.log
	* org.slf4j
	* org.slf4j
	* org.slf4j
	* org.slf4j
* __[liveSense :: Core - org.liveSense.core (2-SNAPSHOT)](http://github.com/liveSense/org.liveSense.core)__
	* org.liveSense.core
	* org.liveSense.core.service
* __Apache ServiceMix :: Specs :: Java Persistence API 1.4 - org.apache.servicemix.specs.java-persistence-api-1.1.1 (2.2.0)__
	* javax.persistence

## OSGi Embedded JARs

## Dependency Graph
![alt text](http://raw.github.com.everydayimmirror.in/liveSense/org.liveSense.service.dataSourceProvider/master/osgidependencies.svg "")