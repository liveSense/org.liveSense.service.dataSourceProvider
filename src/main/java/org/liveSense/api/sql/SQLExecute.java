package org.liveSense.api.sql;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.persistence.Column;
import javax.persistence.Id;
import javax.sql.DataSource;

import org.apache.commons.dbcp.BasicDataSource;
import org.apache.commons.dbutils.BasicRowProcessor;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.ResultSetHandler;
import org.apache.commons.dbutils.handlers.BeanListHandler;
import org.liveSense.api.beanprocessors.DbStandardBeanProcessor;
import org.liveSense.api.sql.exceptions.SQLException;
import org.liveSense.misc.queryBuilder.QueryBuilder;
import org.liveSense.misc.queryBuilder.SimpleSQLQueryBuilder;
import org.liveSense.misc.queryBuilder.criterias.EqualCriteria;
import org.liveSense.misc.queryBuilder.exceptions.QueryBuilderException;
import org.liveSense.misc.queryBuilder.jdbcDriver.JdbcDrivers;
import org.liveSense.misc.queryBuilder.operators.AndOperator;


/**
 * This class provides basic functionalities of SQL for javax.persistence annotated beans.
 * It's a simple CRUD based persistance layer.
 * 
 * @param <T> - The Bean class is used for
 */
public abstract class SQLExecute<T> {
	protected QueryBuilder builder;
	
	private enum StatementType {
		INSERT, UPDATE
	}
	
	private PreparedStatement preparedStatement = null;
	private StatementType preparedType = null;
	private Class<?> preparedStatementClass = null;
	private Connection connection = null;
	private ArrayList<String> prepareStatementElements = null;
	private String jdbcDriverClass;

		
	public String getJdbcDriverClass() {	
		return jdbcDriverClass;
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
	 * {@inheritDoc}
	 * @param tableAlias SQL alias of the table 
	 * @see {@link SQLExecute#getSelectQuery()}
	 */
	public abstract String getSelectQuery(String tableAlias) throws SQLException, QueryBuilderException;
	
	
	public abstract String getLockQuery() throws SQLException, QueryBuilderException;
		
	public abstract String getLockQuery(String tableAlias) throws SQLException, QueryBuilderException;	
		
	/**
	 * Get the related SQL dialect by the DataSource (Apache DBCP required).
	 * The supported engines are: MYSQL, HSQLDB, FIREBIRD
	 * @param ds The dataSource object
	 * @return SQL Execute Object (optimized for dialect)
	 * @throws SQLException
	 */
	@SuppressWarnings("rawtypes")
	public static SQLExecute<?> getExecuterByDataSource(BasicDataSource ds) throws SQLException {
		if (ds == null) throw new SQLException("No datasource");
		
		String driver = ds.getDriverClassName();
		
		SQLExecute<?> executer;
		if (driver.equals(JdbcDrivers.MYSQL.getDriverClass())) executer = new MySqlExecute(-1);
		else if (driver.equals(JdbcDrivers.HSQLDB.getDriverClass())) executer = new HSqlDbExecute(-1);
		else if (driver.equals(JdbcDrivers.FIREBIRD.getDriverClass())) executer = new FirebirdExecute(-1);
		else throw new SQLException("This type of JDBC dialect is not implemented: "+driver);
		executer.jdbcDriverClass = driver;
		return executer;
	}
	
	/**
	 * Query entites from database. The query builded by the given QueryBuilder. The resulset mapped by
	 * defult with the Bean javax.persistence.Column annotation, if annotation is not found the field names
	 * is the resultset column name. (The _ character are deleted)
	 * 
	 * @param dataSource The datasource
	 * @param targetClass
	 * @param builder Query builder
	 * @return List of bean objects
	 * @throws Exception
	 */
	public List<T> queryEntities(DataSource dataSource, 
			Class<T> targetClass, QueryBuilder builder) throws Exception {
		return queryEntities(dataSource.getConnection(), targetClass, builder);
	}
	
	/**
	 * {@inheritDoc}
	 * @param tableAlias SQL alias of the table 
	 * @see {@link SQLExecute#queryEntities(DataSource dataSource, Class<T> targetClass, QueryBuilder builder)}
	 */	
	public List<T> queryEntities(DataSource dataSource, 
		Class<T> targetClass, String tableAlias, QueryBuilder builder) throws Exception {
	return queryEntities(dataSource.getConnection(), targetClass, tableAlias, builder);
}
	
	/**
	 * Query entites from database. The query builded by the given QueryBuilder. The resulset mapped by
	 * defult with the Bean javax.persistence.Column annotation, if annotation is not found the field names
	 * is the resultset column name. (The _ character are deleted)
	 * 
	 * @param Connection SQL Connection
	 * @param targetClass
	 * @param builder Query builder
	 * @return List of bean objects
	 * @throws Exception
	 */
	public List<T> queryEntities(Connection connection, 
			Class<T> targetClass, QueryBuilder builder) throws Exception {
		return queryEntities(connection, targetClass, "", builder);
	}

	/**
	 * {@inheritDoc}
	 * @param tableAlias SQL alias of the table 
	 * @see {@link SQLExecute#queryEntities(Connection connection, Class<T> targetClass, QueryBuilder builder)}
	 */
	public List<T> queryEntities(Connection connection, 
			Class<T> targetClass, String tableAlias, QueryBuilder builder) throws Exception {		
		this.builder = builder;
		// TODO Templateket kezelni
		QueryRunner run = new QueryRunner();
		ResultSetHandler<List<T>> rh = new BeanListHandler<T>(targetClass, new BasicRowProcessor(new DbStandardBeanProcessor()));		
		return run.query(connection, getSelectQuery(tableAlias), rh);
	}
	
	/**
	 * locks entites in database. The query builded by the given QueryBuilder. The resulset mapped by
	 * defult with the Bean javax.persistence.Column annotation, if annotation is not found the field names
	 * is the resultset column name. (The _ character are deleted)
	 * 
	 * @param Connection SQL Connection
	 * @param targetClass
	 * @param builder Query builder
	 * @return List of bean objects
	 * @throws Exception
	 */
	public List<T> lockEntities(Connection connection, 
			Class<T> targetClass, QueryBuilder builder) throws Exception {
		return lockEntities(connection, targetClass, "", builder);
	}

	/**
	 * {@inheritDoc}
	 * @param tableAlias SQL alias of the table 
	 * @see {@link SQLExecute#lockEntities(Connection connection, Class<T> targetClass, QueryBuilder builder)}
	 */
	public List<T> lockEntities(Connection connection, 
			Class<T> targetClass, String tableAlias, QueryBuilder builder) throws Exception {		
		this.builder = builder;
		// TODO Templateket kezelni
		QueryRunner run = new QueryRunner();
		ResultSetHandler<List<T>> rh = new BeanListHandler<T>(targetClass, new BasicRowProcessor(new DbStandardBeanProcessor()));
		
		return run.query(connection, getLockQuery(tableAlias), rh);
	}
	
	@SuppressWarnings("unchecked")
	public void lockEntity(Connection connection, T entity) throws Exception {
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
		
		QueryBuilder builder = new SimpleBeanSQLQueryBuilder(entity.getClass()); 
		builder.setParams(new AndOperator(new EqualCriteria<Integer>("id", (Integer) objs.get(idColumn))));
		lockEntities(connection, (Class<T>) entity.getClass(), builder);		
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
	 * Delete entities 
	 * @param connection SQL Connection
	 * @param clazz
	 * @param Object condition SQL condition for QueryBuilder.setParams  
	 * @throws Exception
	 */	
	public void deleteEntities(Connection connection, Class<T> clazz, Object condition) throws Exception {
		deleteEntities(connection, clazz, "", condition);
	}

	/**
	 * {@inheritDoc}
	 * @param tableAlias SQL alias of the table 
	 * @see {@link SQLExecute#deleteEntities(Connection connection, Class<T> clazz, Object condition)}
	 */	
	public void deleteEntities(Connection connection, Class<T> clazz, String tableAlias, Object condition) throws Exception {
		if (connection == null) throw new SQLException("Connection is null");
		String tableName = AnnotationHelper.getTableName(clazz);
		if (tableName == null || "".equalsIgnoreCase(tableName)) {
			throw new SQLException("Entity does not contain javax.persistence.Entity annotation");
		}
		StringBuffer sb = new StringBuffer();
		sb.append("DELETE FROM "+tableName+" "+tableAlias);
		if (condition != null) {
			QueryBuilder builder = new SimpleSQLQueryBuilder("");
			builder.setParams(condition);
			sb.append(" WHERE "+builder.buildParameters());
		}
		PreparedStatement stm = connection.prepareStatement(sb.toString());
		stm.execute();
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
			if (objs.get(key) != null) {
				if (!first) {sb.append(","); sb2.append(",");} else first = false;
				sb.append(key);
				sb2.append("?");
			}
		}
		PreparedStatement stm = connection.prepareStatement(sb.toString()+") VALUES "+sb2.toString()+")");
		int idx = 1;
		for (Object param : objs.values()) {
			if (param != null) {
				if (param instanceof java.util.Date) {
					java.sql.Date paramD = new java.sql.Date(((java.util.Date)param).getTime());
					param = paramD;
				}
				stm.setObject(idx, param);
				idx++;
			}
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
		updateEntity(connection, entity, (List<String>)null);
	}
	
	/**
	 * {@inheritDoc}
	 * @param fields list of the updateabe fields
	 * @see {@link SQLExecute#updateEntity(Connection connection, T entity)}
	 */	
	public void updateEntity(Connection connection, T entity, String[] fields) throws Exception {
		List<String> list = new ArrayList<String>(Arrays.asList(fields));		
		updateEntity(connection, entity, list);
	}	
	
	/**
	 * {@inheritDoc}
	 * @param fields list of the updateabe fields
	 * @see {@link SQLExecute#updateEntity(Connection connection, T entity)}
	 */	
	public void updateEntity(Connection connection, T entity, List<String> fields) throws Exception {
		String idColumn = AnnotationHelper.getIdColumnName(entity);
		Map<String, Object> objs = AnnotationHelper.getObjectAsMap(entity, fields);
		
		updateEntities(connection, entity, "", fields, new AndOperator(new EqualCriteria<Integer>("id", new Integer((Integer) objs.get(idColumn)))));	
	}
	
	/**
	 * Update entities.
	 * @param connection SQL Connection
	 * @param entity The bean
	 * @param fields list of the updateabe fields
	 * @param Object condition SQL condition for QueryBuilder.setParams
	 * @throws Exception
	 */
	public void updateEntities(Connection connection, T entity, List<String> fields, Object condition) throws Exception {
		updateEntities(connection, entity, "", fields, condition);
	}
	 
	/**
	 * {@inheritDoc}
	 * @see {@link updateEntities#updateEntity(Connection connection, T entity, List<String> fields, Object condition)}
	*/		 
	public void updateEntities(Connection connection, T entity, String[] fields, Object condition) throws Exception {
		List<String> list = new ArrayList<String>(Arrays.asList(fields));
		updateEntities(connection, entity, "", list, condition);
	}
	 
	/**
	 * {@inheritDoc}
	 * @param tableAlias SQL alias of the table 
	 * @see {@link updateEntities#updateEntity(Connection connection, T entity, List<String> fields, Object condition)}
	 */	 	 
	public void updateEntities(Connection connection,T entity, String tableAlias, String[] fields, Object condition) throws Exception {
		List<String> list = new ArrayList<String>(Arrays.asList(fields));
		updateEntities(connection, entity, tableAlias, list, condition);
	}

	/**
	 * {@inheritDoc}
	 * @param tableAlias SQL alias of the table 
	 * @see {@link updateEntities#updateEntity(Connection connection, T entity, List<String> fields, Object condition)}
	 */	 
	public void updateEntities(Connection connection,T entity, String tableAlias, List<String> fields, Object condition) throws Exception {
		String idColumn = AnnotationHelper.getIdColumnName(entity);
		String tableName = AnnotationHelper.getTableName(entity);		
		if (tableName == null || "".equalsIgnoreCase(tableName)) {
			throw new SQLException("Entity does not contain javax.persistence.Entity annotation");
		}
		if (idColumn == null || "".equalsIgnoreCase(idColumn)) {
			throw new SQLException("Entity does not contain javax.persistence.Id annotation");
		}		
		if ((fields != null) && (fields.size() > 0)) {
			String idFieldName = AnnotationHelper.findFieldByAnnotationClass(entity.getClass(),Column.class).getName();
			fields.add(idFieldName);	
		}
		Map<String, Object> objs = AnnotationHelper.getObjectAsMap(entity, fields);
		StringBuffer sb = new StringBuffer();
		sb.append("UPDATE "+tableName+" "+tableAlias+" SET ");
		boolean first = true;
		for (String key : objs.keySet()) {
			if (!key.equals(idColumn)) {
				if (!first) {sb.append(",");} else first = false;
				if (tableAlias.equals("")) {
					sb.append(key+" = ?");
				}
				else {
					sb.append(tableAlias+"."+key+" = ?");
				}
			}			
		}	
		if (condition != null) {
			QueryBuilder builder = new SimpleSQLQueryBuilder("");
			builder.setParams(condition);
			sb.append(" WHERE "+builder.buildParameters());
		}
		PreparedStatement stm = connection.prepareStatement(sb.toString());
		int idx = 1;
		for (String key : objs.keySet()) {
			if (!key.equals(idColumn)) {
				if (objs.get(key) instanceof java.util.Date) {
					java.sql.Date paramD = new java.sql.Date(((java.util.Date)objs.get(key)).getTime());
					stm.setObject(idx, paramD);
				} else {
					stm.setObject(idx, objs.get(key));
				}
				idx++;
			}
		}
		stm.execute();		
	}	
	
	/**
	 * Prepare an insert statement to execute the statement several times with different objects.
	 * @param connection
	 * @param targetClass 
	 * @throws Exception
	 */
	public void prepareInsertStatement(Connection connection, Class<T> targetClass) throws Exception {
		prepareInsertStatement(connection,targetClass,(List<String>)null);
	}	
	
	/**
	 * {@inheritDoc}
	 * @param fields list of fields  
	 * @see {@link updateEntities#prepareInsertStatement(Connection connection, Class<T> targetClass)}
	 */		
	public void prepareInsertStatement(Connection connection, Class<T> targetClass, String[] fields) throws Exception {
		List<String> list = new ArrayList<String>(Arrays.asList(fields));
		prepareInsertStatement(connection,targetClass,list);
	}
	
	/**
	 * {@inheritDoc}
	 * @param fields list of fields  
	 * @see {@link updateEntities#prepareInsertStatement(Connection connection, Class<T> targetClass)}
	 */	
	public void prepareInsertStatement(Connection connection, Class<T> targetClass, List<String> fields) throws Exception {
		preparedType = StatementType.INSERT;
		preparedStatementClass = targetClass;
		Set<String> columns = AnnotationHelper.getClassColumnNames(targetClass, fields);
		String tableName = AnnotationHelper.getTableName(targetClass);
		if (tableName == null || "".equalsIgnoreCase(tableName)) {
			throw new SQLException("Entity does not contain javax.persistence.Entity annotation");
		}
		StringBuffer sb = new StringBuffer();
		StringBuffer sb2 = new StringBuffer();
		sb.append("INSERT INTO "+AnnotationHelper.getTableName(targetClass)+" (");
		sb2.append("(");
		boolean first = true;
		prepareStatementElements = new ArrayList<String>();
		for (String columnName : columns) {
			prepareStatementElements.add(columnName);
			if (!first) {sb.append(","); sb2.append(",");} else first = false;
			sb.append(columnName);
			sb2.append("?");
		}
		preparedStatement = connection.prepareStatement(sb.toString()+") VALUES "+sb2.toString()+")");
		this.connection = connection;
	}

	/**
	 * Prepare an update statement to execute the statement several times with different objects.
	 * @param connection
	 * @param targetClass
	 * @throws Exception
	 */
	public void prepareUpdateStatement(Connection connection, Class<T> targetClass) throws Exception {
		prepareUpdateStatement(connection, targetClass, (List<String>)null);
	}
	
	/**
	 * {@inheritDoc}
	 * @param fields list of fields  
	 * @see {@link updateEntities#prepareUpdateStatement(Connection connection, Class<T> targetClass)}
	 */	
	public void prepareUpdateStatement(Connection connection, Class<T> targetClass, String[] fields) throws Exception {
		List<String> list =  new ArrayList<String>(Arrays.asList(fields));
		prepareUpdateStatement(connection, targetClass, list);
	}
	
	/**
	 * {@inheritDoc}
	 * @param fields list of fields  
	 * @see {@link updateEntities#prepareUpdateStatement(Connection connection, Class<T> targetClass)}
	 */	
	public void prepareUpdateStatement(Connection connection, Class<T> targetClass, List<String> fields) throws Exception {
		preparedType = StatementType.UPDATE;
		preparedStatementClass = targetClass;
		Set<String> columns = AnnotationHelper.getClassColumnNames(targetClass, fields);
		String idColumn = AnnotationHelper.getIdColumnName(targetClass);
		String tableName = AnnotationHelper.getTableName(targetClass);
		if (tableName == null || "".equalsIgnoreCase(tableName)) {
			throw new SQLException("Entity does not contain javax.persistence.Entity annotation");
		}
		if (idColumn == null || "".equalsIgnoreCase(idColumn)) {
			throw new SQLException("Entity does not contain javax.persistence.Id annotation");
		}
		StringBuffer sb = new StringBuffer();
		sb.append("UPDATE "+tableName+" SET ");
		boolean first = true;
		prepareStatementElements = new ArrayList<String>();
		for (String columnName : columns) {
			if (!columnName.equals(idColumn)) {
				if (!first) {sb.append(",");} else first = false;
				prepareStatementElements.add(columnName);
				sb.append(columnName+" = ?");
			}
		}	
		sb.append(" WHERE "+idColumn+" = ?");
		preparedStatement = connection.prepareStatement(sb.toString());
		this.connection = connection;
	}
	
	/**
	 * Insert one entity with prepared statement. The given bean have to be annotated with javax.persistence.Entity and javax.persistence.Id
	 * @param entity The bean
	 * @throws Exception
	 */
	public void insertEntityWithPreparedStatement(T entity) throws Exception {
		if (preparedType == null || preparedStatementClass == null || preparedStatement == null || this.connection == null) throw new SQLException("The statement is not prepared");
		if (preparedType != StatementType.INSERT) throw new SQLException("The statement type does not match with INSERT");
		if (entity.getClass() != preparedStatementClass) throw new SQLException("Entity class type mismatch");
		
		Map<String, Object> objs = AnnotationHelper.getObjectAsMap(entity);
		int idx = 1;
		for (String field : prepareStatementElements) {
			Object param = objs.get(field);
			if (param instanceof java.util.Date) {
				java.sql.Date paramD = new java.sql.Date(((java.util.Date)param).getTime());
				param = paramD;
			}
			preparedStatement.setObject(idx, param);
			idx++;
		}
		preparedStatement.execute();
		if (preparedStatement.getUpdateCount() != 1) {
			throw new java.sql.SQLException("INSERT was unsuccessfull");
		}
	}
	
	/**
	 * Update one entity with prepared statement. The given bean have to be annotated with javax.persistence.Entity and javax.persistence.Id
	 * @param entity The bean
	 * @throws Exception
	 */
	public void updateEntityWithPreparedStatement(T entity) throws Exception {
		if (preparedType == null || preparedStatementClass == null || preparedStatement == null || this.connection == null) throw new SQLException("The statement is not prepared");
		if (preparedType != StatementType.INSERT) throw new SQLException("The statement type does not match with INSERT");
		if (entity.getClass() != preparedStatementClass) throw new SQLException("Entity class type mismatch");

		Map<String, Object> objs = AnnotationHelper.getObjectAsMap(entity);
		String idColumn = AnnotationHelper.getIdColumnName(entity);
		int idx = 1;
		for (String key : prepareStatementElements) {
			if (!key.equals(idColumn)) {
				if (objs.get(key) instanceof java.util.Date) {
					java.sql.Date paramD = new java.sql.Date(((java.util.Date)objs.get(key)).getTime());
					preparedStatement.setObject(idx, paramD);
				} else {
					preparedStatement.setObject(idx, objs.get(key));
				}
				idx++;
			}
		}
		preparedStatement.setObject(idx, objs.get(idColumn));
		preparedStatement.execute();
		if (preparedStatement.getUpdateCount() != 1) {
			throw new java.sql.SQLException("UPDATE was unsuccessfull");
		}		
	}
	
	@SuppressWarnings("rawtypes")
	public void insertSelect(
		Connection connection, 
		Class<T> insertClass, String[] insertFields, 
		Class selectClass, String tableAlias, String[] selectFields, Object selectCondition) 
		throws java.sql.SQLException, SQLException, IllegalAccessException, InvocationTargetException, NoSuchMethodException, QueryBuilderException{
				
		List<String> list1 = new ArrayList<String>(Arrays.asList(insertFields));
		List<String> list2 = new ArrayList<String>(Arrays.asList(selectFields));
		insertSelect(connection, insertClass, list1, selectClass, tableAlias, list2, selectCondition);
	}
	
	
	@SuppressWarnings("rawtypes")
	public void insertSelect(
		Connection connection, 
		Class<T> insertClass, List<String> insertFields, 
		Class selectClass, String tableAlias, List<String> selectFields, Object selectCondition) 
		throws java.sql.SQLException, SQLException, IllegalAccessException, InvocationTargetException, NoSuchMethodException, QueryBuilderException{
		
		String insertTableName = AnnotationHelper.getTableName(insertClass);		
		if (insertTableName == null || "".equalsIgnoreCase(insertTableName)) {
			throw new SQLException("Entity does not contain javax.persistence.Entity annotation");
		}		
		String selectTableName = AnnotationHelper.getTableName(selectClass);		
		if (selectTableName == null || "".equalsIgnoreCase(selectTableName)) {
			throw new SQLException("Entity does not contain javax.persistence.Entity annotation");
		}		
		//insert
		ArrayList<String> insertColumns = AnnotationHelper.getClassColumnNames(insertClass, insertFields, true);
		StringBuffer sb = new StringBuffer();
		sb.append("INSERT INTO "+insertTableName+" (");
		boolean first = true;
		for (String columnName : insertColumns) {
			if (!first) {
				sb.append(","); 
			} 
			else first = false;
			sb.append(columnName);
		}
		sb.append(")");
		String insert = sb.toString();
		//select
		ArrayList<String> selectColumns = AnnotationHelper.getClassColumnNames(selectClass, insertFields, true);
		sb = new StringBuffer();
		first = true;
		for (String columnName : selectColumns) {
			if (!first) {
				sb.append(","); 
			} 
			else first = false;
			if (tableAlias.equals("")) {
				sb.append(columnName);
			} else {
				sb.append(tableAlias+"."+columnName);
			}
		}	
		this.builder = new SimpleSQLQueryBuilder("SELECT " + sb.toString() +" FROM "+selectTableName);
		if (selectCondition != null) {
			this.builder.setParams(selectCondition);
		}
		String select = getSelectQuery(tableAlias).replace("*", sb.toString());
		//
		PreparedStatement stm = connection.prepareStatement(insert +"\n"+ select);
		stm.execute();		
	}
			
	/**
	 * Create table. The given bean have to be annotated with javax.persistence.Entity, javax.presistence.Column and javax.persistence.Id
	 * @param connection SQL Connection
	 * @param class The entity bean class
	 * @throws Exception
	 */
	public void createTable(Connection connection, Class<T> clazz) throws Exception {
		
		
		String tableName = AnnotationHelper.getTableName(clazz);
		if (tableName == null || "".equalsIgnoreCase(tableName)) {
			throw new SQLException("Class does not contain javax.persistence.Entity annotation");
		}
		
		List<Field> fields = AnnotationHelper.getAllFields(clazz);
		StringBuffer sb = new StringBuffer();
		sb.append("CREATE TABLE "+tableName+" (");

		boolean firstField = true;
    	for (Field fld : fields) {
    		Annotation[] annotations = fld.getAnnotations();
			Id id = null;
			Column col = null;

    		for (int i=0; i<annotations.length; i++) {
    			if (annotations[i] instanceof Column) {
    				col = (Column)annotations[i];
    			} else if (annotations[i] instanceof Id) {
    				id = (Id)annotations[i];
    			}
    		}
			if (col != null) {
				if (firstField) firstField = false; else sb.append(",");
    			if (col.name() == null || col.name().equals("")) throw new SQLException("Column name is undefined");
    			if (col.columnDefinition() == null || col.columnDefinition().equals("")) throw new SQLException("Column definition is undefined");
    			sb.append(col.name());
    			sb.append(" "+col.columnDefinition());
    			if (!col.nullable()) {
    				sb.append(" NOT NULL");
    			}
    			if (col.unique()) {
    				sb.append(" UNIQUE");
    			}
    			if (id != null) {
    				sb.append(" PRIMARY KEY");
    			}
			}
    	}
    	sb.append(")");
		PreparedStatement stm = connection.prepareStatement(sb.toString());
		stm.execute();
		connection.commit();
	}
	
	/**
	 * Drop table. The given bean have to be annotated with javax.persistence.Entity
	 * @param connection SQL Connection
	 * @param class The entity bean class
	 * @throws Exception
	 */
	public void dropTable(Connection connection, Class<T> clazz) throws Exception {
		
		
		String tableName = AnnotationHelper.getTableName(clazz);
		if (tableName == null || "".equalsIgnoreCase(tableName)) {
			throw new SQLException("Class does not contain javax.persistence.Entity annotation");
		}
		
		StringBuffer sb = new StringBuffer();
		sb.append("DROP TABLE "+tableName);		
		PreparedStatement stm = connection.prepareStatement(sb.toString());
		stm.execute();
	}

	


	/**
	 * Check if the given table exists. The given bean have to be annotated with javax.persistence.Entity
	 * @param connection SQL Connection
	 * @param class The entity bean class
	 * @throws Exception
	 */
	public boolean existsTable(Connection connection, Class<T> clazz) throws Exception {
		
		
		String tableName = AnnotationHelper.getTableName(clazz);
		if (tableName == null || "".equalsIgnoreCase(tableName)) {
			throw new SQLException("Class does not contain javax.persistence.Entity annotation");
		}
		
		DatabaseMetaData dbm = connection.getMetaData();
		ResultSet tables = dbm.getTables((String)null, (String)null, tableName, (String[])null);

		if (tables.next()) {
			// Table exists
			return true;
		}
		else {
			// Table does not exist
			return false;
		}
	}

	/**
	 * Execute an SQL Script. The script contains only a single statement. 
	 * @param connection
	 * @param sql
	 * @throws SQLException
	 */
	public void executeScript(Connection connection, File sql) throws SQLException {
		executeScript(connection, sql, null, null);
	}
	
	/**
	 * Execute an SQL Script. The statements are separated with ;
	 * @param connection
	 * @param sql
	 * @param section
	 * @throws SQLException
	 */
	public void executeScript(Connection connection, File sql, String section) throws SQLException {
		executeScript(connection, sql, section, ";");
	}
	
	/**
	 * Execute an SQL Script. The statements are separated with the given string.
	 * @param connection
	 * @param sql
	 * @param section
	 * @param separator
	 * @throws SQLException
	 */
	public void executeScript(Connection connection, File sql, String section, String separator) throws SQLException {
		String s            = new String();  
		StringBuffer sb = new StringBuffer();
		
		String actSection = null;
	
	    try {
			FileReader fr = new FileReader(sql);   		
		    BufferedReader br = new BufferedReader(fr);  

	    	while((s = br.readLine()) != null)  
			{  
	    		if (s.trim().startsWith("@")) {
	    			actSection = s.trim().substring(1);
	    		} else {
	    			boolean use = true;
	    			if (section != null) use = false;
	    			if (section != null && actSection != null && actSection.equalsIgnoreCase(section)) 
	    				use = true;
	    			if (use) sb.append(s+"\n");
	    		}
	    			
	    	
	    			
			}
		    br.close();  

	    }
		catch (IOException e) {
			throw new SQLException(e);
		}  

		
		//begin the sql file parser to separate the sql commands into  
        //separate array entries. This parser requires that your  
        //sql statements be typed in uppercase because that is the   
        //convention of the author.  
		
		//Step 1: Split script to commands when needed
		String[] stmts = null;
		if (separator != null) {
			stmts = sb.toString().split(separator);
		} else {
			stmts = new String[] { sb.toString() }; 
		}
		

        //Step 2: Put Transactions back into a single statement.  
        for(int i=0;i<stmts.length;i++){  
            //if the current statement starts a transaction  
            if(stmts[i].contains("BEGIN TRANSACTION")){  
                int tInt = i;  
                //find the end of the transaction or the end of the file  
                //whichever comes first  
                while(tInt<stmts.length && !stmts[tInt].contains("END TRANSACTION")) {  
                    tInt++;  
                } //end while  

                //add a semicolon to the first sql entry in the transaction  
                //which will be in the same array entry as the BEGIN  
                //statement  
                stmts[i] += separator;  

                //loop through the remaining transaction and place them  
                //into the transaction start entry appending semicolons  
                //at the end of each statement  
                for(int j = (i+1); j< tInt; j++) {  
                    stmts[i] += "\n" + stmts[j] + separator;  
                    //blank out the current transaction entry so that the  
                    //executer skips it  
                    stmts[j] = " ";  
                } //end for  

                //and the end statement to the end of the transaction  
                stmts[i] += "\nEND TRANSACTION";  

                //remove the END transaction from the statement it is  
                //currently embedded in  
                String tStr[] = stmts[tInt].split("END TRANSACTION"); 
                if (tStr.length>0)
                	stmts[tInt] = tStr[1];
                else
                	stmts[tInt] = "";
                //skip the statements blanked out earlier, actually pointing  
                //to the last transaction entry so that the for statement  
                //points to the first statement after the transaction  
                i = tInt - 1;
            } //end if              
        } //end for  

        // Removes BEGIN and END with
        for (int i=0; i<stmts.length; i++)
        	stmts[i] = stmts[i].replaceAll("BEGIN TRANSACTION", "").replaceAll("END TRANSACTION", "COMMIT");


        //end sql file parsers  

        // Executing commands
        for (int si = 0; si<stmts.length; si++) {
        	String[] inst = null;
        	if (separator != null) {
        		inst = stmts[si].split(separator);
        	} else {
        		inst = new String[] { stmts[si] };
        	}
		    
		    Statement st;
			try {
				st = connection.createStatement();
		        for(int i = 0; i<inst.length; i++)  
		        {  
		            // we ensure that there is no spaces before or after the request string  
		            // in order to not execute empty statements  
		            if(!inst[i].trim().equals(""))  
		            {  
		                if (inst[i].trim().toUpperCase().startsWith("COMMIT")) {
		                	connection.commit();
		                } else {
		                	st.executeUpdate(inst[i]);  
		                }
		            }  
		        }
			}
			catch (java.sql.SQLException e) {
				throw new SQLException(e);
			}  
        }
	}
}