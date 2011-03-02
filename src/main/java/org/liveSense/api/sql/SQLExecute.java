package org.liveSense.api.sql;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.List;
import java.util.Map;

import javax.sql.DataSource;

import org.apache.commons.dbcp.BasicDataSource;
import org.apache.commons.dbutils.BasicRowProcessor;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.ResultSetHandler;
import org.apache.commons.dbutils.handlers.BeanListHandler;
import org.liveSense.api.beanprocessors.DbStandardBeanProcessor;
import org.liveSense.api.sql.exceptions.SQLException;
import org.liveSense.misc.queryBuilder.QueryBuilder;
import org.liveSense.misc.queryBuilder.exceptions.QueryBuilderException;


/**
 * This class provides basic functionalities of SQL for javax.persistence annotated beans.
 * It's a simple CRUD based persistance layer.
 * 
 * @param <T> - The Bean class is used for
 */
public abstract class SQLExecute<T> {
	protected QueryBuilder builder;

	public enum JdbcDrivers {
		MYSQL ("com.mysql.jdbc.Driver"),
		FIREBIRD ("org.firebirdsql.jdbc.FBDriver"),
		HSQLDB ("org.hsqldb.jdbcDriver");
		
		private final String driverClass;
		
		private JdbcDrivers(String driverClass) {
			this.driverClass = driverClass;
		}
		
		public String getDriverClass() {
			return this.driverClass;
		}
	}
	
	
	/**
	 * This class is a transport class which contains and builds the query. 
	 *
	 */
	public class ClauseHelper {
		private String query;
		private Boolean subSelect;
		
		public ClauseHelper(String query, Boolean subSelect) {
			this.query = query;
			this.subSelect = subSelect;
		}
		
		public String getQuery() {
			return query;
		}
		public void setQuery(String query) {
			this.query = query;
		}
		public Boolean getSubSelect() {
			return subSelect;
		}
		public void setSubSelect(Boolean subSelect) {
			this.subSelect = subSelect;
		}
		
	}

	/**
	 * Add where clause for select. The method depends on the type of SQL dialect
	 * @param helper
	 * @return
	 * @throws SQLException
	 * @throws QueryBuilderException
	 */
	public abstract ClauseHelper addWhereClause(ClauseHelper helper) throws SQLException, QueryBuilderException;

	/**
	 * Add limit clause for select. The method depends on the type of SQL dialect
	 * @param helper
	 * @return
	 * @throws SQLException
	 * @throws QueryBuilderException
	 */
	public abstract ClauseHelper addLimitClause(ClauseHelper helper) throws SQLException, QueryBuilderException;

	/**
	 * Add order by clause for select. The method depends on the type of SQL dialect
	 * @param helper
	 * @return
	 * @throws SQLException
	 * @throws QueryBuilderException
	 */
	public abstract ClauseHelper addOrderByClause(ClauseHelper helper) throws SQLException, QueryBuilderException;

	/**
	 * It's build the statement from the base query by added the clauses.
	 * @return The final SQL statement
	 * @throws SQLException
	 * @throws QueryBuilderException
	 */
	public abstract String getSelectQuery() throws SQLException, QueryBuilderException;
	
	
	/**
	 * Get the related SQL dialect by the DataSource (Apache DBCP required).
	 * The supported engines is: MYSQL, HSQLDB, FIREBIRD
	 * @param ds The dataSource object
	 * @return SQL Execute Object (optimized for dialect)
	 * @throws SQLException
	 */
	@SuppressWarnings("rawtypes")
	public static SQLExecute<?> getExecuterByDataSource(BasicDataSource ds) throws SQLException {
		if (ds == null) throw new SQLException("No datasource");
		String driverClass = ds.getDriverClassName();
		
		if (driverClass.equals(JdbcDrivers.MYSQL.driverClass)) return new MySqlExecute(-1);
		else if (driverClass.equals(JdbcDrivers.HSQLDB.driverClass)) return new HSqlDbExecute(-1);
		else if (driverClass.equals(JdbcDrivers.FIREBIRD.driverClass)) return new FirebirdExecute(-1);
		else throw new SQLException("This type of JDBC dialect is not implemented: "+driverClass);
	}
	
	/**
	 * Query entites from database. The query builded by the given QueryBuilder. The resulset mapped by
	 * defult with the Bean javax.persistence.Column annotation, if annotation is not found the field names
	 * is the resultset column name. (The _ character are deleted)
	 * 
	 * @param dataSource The datasource
	 * @param targetClass The target Bean object.
	 * @param builder Query builder
	 * @return List of bean objects
	 * @throws Exception
	 */
	public List<T> queryEntities(DataSource dataSource, 
			Class<T> targetClass, QueryBuilder builder) throws Exception {
		return queryEntities(dataSource.getConnection(), targetClass, builder);
	}

	/**
	 * Query entites from database. The query builded by the given QueryBuilder. The resulset mapped by
	 * defult with the Bean javax.persistence.Column annotation, if annotation is not found the field names
	 * is the resultset column name. (The _ character are deleted)
	 * 
	 * @param Connection SQL Connection
	 * @param targetClass The target Bean object.
	 * @param builder Query builder
	 * @return List of bean objects
	 * @throws Exception
	 */

	public List<T> queryEntities(Connection connection, 
			Class<T> targetClass, QueryBuilder builder) throws Exception {		
		this.builder = builder;
		// TODO Templateket kezelni
		QueryRunner run = new QueryRunner();
		ResultSetHandler<List<T>> rh = new BeanListHandler<T>(targetClass, new BasicRowProcessor(new DbStandardBeanProcessor()));		
		return run.query(connection, getSelectQuery(), rh);
	}
	
	/**
	 * Delete one entity. The given bean have to be annotated with javax.persistence.Entity and javax.persistence.Id
	 * @param connection SQL Connection
	 * @param entity The bean
	 * @throws Exception
	 */
	public void deleteEntity(Connection connection, T entity) throws Exception {
		if (connection == null) throw new SQLException("Connection is null");
		if (entity == null) throw new SQLException("Entity is null");
		Map<String, Object> objs = AnnotationHelper.getObjectAsMap(entity);
		String idColumn = AnnotationHelper.getIdColumnName(entity);
		String tableName = AnnotationHelper.getTableName(entity);
		if (tableName == null || "".equalsIgnoreCase(tableName)) {
			throw new SQLException("Entity does not contain javax.persistence.Entity annotation");
		}
		if (idColumn == null || "".equalsIgnoreCase(idColumn)) {
			throw new SQLException("Entity does not contain javax.persistence.Id annotation");
		}
		StringBuffer sb = new StringBuffer();
		sb.append("DELETE FROM "+tableName+" ");
		sb.append(" WHERE "+idColumn+" = ?");
		PreparedStatement stm = connection.prepareStatement(sb.toString());
		int idx = 1;
		stm.setObject(idx, objs.get(idColumn));
		stm.execute();
		if (stm.getUpdateCount() != 1) {
			throw new java.sql.SQLException("DELETE was unsuccessfull");
		}
	}

	/**
	 * Insert one entity. The given bean have to be annotated with javax.persistence.Entity and javax.persistence.Id
	 * @param connection SQL Connection
	 * @param entity The bean
	 * @throws Exception
	 */
	public void insertEntity(Connection connection, T entity) throws Exception {
		Map<String, Object> objs = AnnotationHelper.getObjectAsMap(entity);
		String tableName = AnnotationHelper.getTableName(entity);
		if (tableName == null || "".equalsIgnoreCase(tableName)) {
			throw new SQLException("Entity does not contain javax.persistence.Entity annotation");
		}
		StringBuffer sb = new StringBuffer();
		StringBuffer sb2 = new StringBuffer();
		sb.append("INSERT INTO "+AnnotationHelper.getTableName(entity)+" (");
		sb2.append("(");
		boolean first = true;
		for (String key : objs.keySet()) {
			if (!first) {sb.append(","); sb2.append(",");} else first = false;
			sb.append(key);
			sb2.append("?");
		}
		PreparedStatement stm = connection.prepareStatement(sb.toString()+") VALUES "+sb2.toString()+")");
		int idx = 1;
		for (Object param : objs.values()) {
			stm.setObject(idx, param);
			idx++;
		}
		stm.execute();
		if (stm.getUpdateCount() != 1) {
			throw new java.sql.SQLException("INSERT was unsuccessfull");
		}
	}
	
	/**
	 * Update one entity. The given bean have to be annotated with javax.persistence.Entity and javax.persistence.Id
	 * @param connection SQL Connection
	 * @param entity The bean
	 * @throws Exception
	 */
	public void updateEntity(Connection connection, T entity) throws Exception {
		Map<String, Object> objs = AnnotationHelper.getObjectAsMap(entity);
		String idColumn = AnnotationHelper.getIdColumnName(entity);
		String tableName = AnnotationHelper.getTableName(entity);
		if (tableName == null || "".equalsIgnoreCase(tableName)) {
			throw new SQLException("Entity does not contain javax.persistence.Entity annotation");
		}
		if (idColumn == null || "".equalsIgnoreCase(idColumn)) {
			throw new SQLException("Entity does not contain javax.persistence.Id annotation");
		}
		StringBuffer sb = new StringBuffer();
		sb.append("UPDATE "+tableName+" SET ");
		boolean first = true;
		for (String key : objs.keySet()) {
			if (!key.equals(idColumn)) {
				if (!first) {sb.append(",");} else first = false;
				sb.append(key+" = ?");
			}
		}	
		sb.append(" WHERE "+idColumn+" = ?");
		PreparedStatement stm = connection.prepareStatement(sb.toString());
		int idx = 1;
		for (String key : objs.keySet()) {
			if (!key.equals(idColumn)) {
				stm.setObject(idx, objs.get(key));
				idx++;
			}
		}
		stm.setObject(idx, objs.get(idColumn));
		stm.execute();
		if (stm.getUpdateCount() != 1) {
			throw new java.sql.SQLException("UPDATE was unsuccessfull");
		}		
	}
	

}