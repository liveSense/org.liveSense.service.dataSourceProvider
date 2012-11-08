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
* __System Bundle - org.apache.felix.framework (3.0.8)__
	* javax.sql
	* org.osgi.framework
* __[liveSense :: Misc :: SQL QueryBuilder - org.liveSense.misc.queryBuilder (2-SNAPSHOT)](http://github.com/liveSense/org.liveSense.misc.queryBuilder)__
	* org.liveSense.misc.queryBuilder
	* org.liveSense.misc.queryBuilder.clauses
	* org.liveSense.misc.queryBuilder.criterias
	* org.liveSense.misc.queryBuilder.domains
	* org.liveSense.misc.queryBuilder.exceptions
	* org.liveSense.misc.queryBuilder.jdbcDriver
	* org.liveSense.misc.queryBuilder.operands
	* org.liveSense.misc.queryBuilder.operators
* __Commons BeanUtils - org.apache.commons.beanutils (1.8.3)__
	* org.apache.commons.beanutils
* __Commons DBCP - org.apache.commons.dbcp (1.4)__
	* org.apache.commons.dbcp
* __Commons DbUtils - org.apache.commons.dbutils (1.4.0)__
	* org.apache.commons.dbutils
	* org.apache.commons.dbutils.handlers
* __slf4j-api - slf4j.api (1.6.1)__
	* org.slf4j
* __Commons Lang - org.apache.commons.lang (2.5)__
	* org.apache.commons.lang
* __Apache Sling OSGi LogService Implementation - org.apache.sling.commons.logservice (1.0.0)__
	* org.osgi.service.log
* __[liveSense :: Core - org.liveSense.core (2-SNAPSHOT)](http://github.com/liveSense/org.liveSense.core)__
	* org.liveSense.core
* __[liveSense :: Extension :: javax.persistence - org.liveSense.misc.javax.persistence (2-SNAPSHOT)](http://github.com/liveSense/org.liveSense.misc.javax.persistence)__
	* javax.persistence
* __Apache Felix Configuration Admin Service - org.apache.felix.configadmin (1.2.8)__
	* org.osgi.service.cm

## OSGi Embedded JARs

## Dependency Graph
![alt text](http://raw.github.com.everydayimmirror.in/liveSense/org.liveSense.service.dataSourceProvider/master/osgidependencies.svg "")