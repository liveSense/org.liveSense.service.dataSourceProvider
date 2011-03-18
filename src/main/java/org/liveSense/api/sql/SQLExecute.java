package org.liveSense.api.sql;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.persistence.Column;
import javax.persistence.Id;
import javax.sql.DataSource;

import org.apache.commons.beanutils.BeanUtilsBean;
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
				if (objs.get(key) instanceof java.util.Date) {
					java.sql.Date paramD = new java.sql.Date(((java.util.Date)objs.get(key)).getTime());
					stm.setObject(idx, paramD);
				} else {
					stm.setObject(idx, objs.get(key));
				}
				idx++;
			}
		}
		stm.setObject(idx, objs.get(idColumn));
		stm.execute();
		if (stm.getUpdateCount() != 1) {
			throw new java.sql.SQLException("UPDATE was unsuccessfull");
		}		
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
	

	public void executeScript(Connection connection, File sql, String section) throws SQLException {
		String s            = new String();  
		StringBuffer sb = new StringBuffer();
		
		String actSection = null;
	
	    try {
			FileReader fr = new FileReader(sql);  
		    // be sure to not have line starting with "--" or "/*" or any other non aplhabetical character  
		
		    BufferedReader br = new BufferedReader(fr);  

	    	while((s = br.readLine()) != null)  
			{  
	    		if (s.trim().startsWith("@")) {
	    			actSection = s.trim().substring(1);
	    		} else if (!s.trim().startsWith("--")) {
	    			boolean use = true;
	    			if (section != null) use = false;
	    			if (section != null && actSection != null && actSection.equalsIgnoreCase(section)) 
	    				use = true;
	    			if (use) sb.append(s);
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
		
		//Step 1: Assume that every SQL statement ends with a semi colon  
        String[] stmts = sb.toString().split(";");  

        //Step 2: Put Transactions back into a single statement.  
        for(int i=0;i<stmts.length;i++){  
            //if the current statement starts a transaction  
            if(stmts[i].contains("BEGIN")){  
                int tInt = i;  
                //find the end of the transaction or the end of the file  
                //whichever comes first  
                while(tInt<stmts.length && !stmts[tInt].contains("END")) {  
                    tInt++;  
                } //end while  

                //add a semicolon to the first sql entry in the transaction  
                //which will be in the same array entry as the BEGIN  
                //statement  
                stmts[i] += ";";  

                //loop through the remaining transaction and place them  
                //into the transaction start entry appending semicolons  
                //at the end of each statement  
                for(int j = (i+1); j< tInt; j++) {  
                    stmts[i] += "\n" + stmts[j] + ";";  
                    //blank out the current transaction entry so that the  
                    //executer skips it  
                    stmts[j] = " ";  
                } //end for  

                //and the end statement to the end of the transaction  
                stmts[i] += "\nEND";  

                //remove the END transaction from the statement it is  
                //currently embedded in  
                String tStr[] = stmts[tInt].split("END"); 
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
        	stmts[i] = stmts[i].replaceAll("BEGIN", "").replaceAll("END", "COMMIT").replaceAll("\n", "");


        //end sql file parsers  

        // Executing commands
        for (int si = 0; si<stmts.length; si++) {
		    String[] inst = stmts[si].split(";");  
		    
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