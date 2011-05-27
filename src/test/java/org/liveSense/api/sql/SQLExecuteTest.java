package org.liveSense.api.sql;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertNotNull;

import java.io.File;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.dbcp.BasicDataSource;
import org.hsqldb.Server;
import org.junit.Ignore;
import org.junit.Test;
import org.liveSense.api.beanprocessors.TestBean;
import org.liveSense.api.beanprocessors.TestBean2;
import org.liveSense.misc.queryBuilder.QueryBuilder;
import org.liveSense.misc.queryBuilder.clauses.OrderByClause;
import org.liveSense.misc.queryBuilder.criterias.BetweenCriteria;
import org.liveSense.misc.queryBuilder.criterias.EqualCriteria;
import org.liveSense.misc.queryBuilder.criterias.GreaterCriteria;
import org.liveSense.misc.queryBuilder.criterias.LessCriteria;
import org.liveSense.misc.queryBuilder.exceptions.QueryBuilderException;
import org.liveSense.misc.queryBuilder.operands.OperandSource;
import org.liveSense.misc.queryBuilder.operators.AndOperator;

public class SQLExecuteTest {

	private BasicDataSource dataSource;
	private Connection connection;
	private Connection connection2;
	private StringBuffer blob = new StringBuffer();
	private Date date = new Date();
	private Date dateWithoutTime = null;
	private TestBean bean;
	private static final String BLOBTEXT = "dfsdfsdf";
	private String dateValue = "'2011-01-03'";
	private String blobValue = "ÁRVÍZTŰRŐTÜKÖRFÚRÓGÉPárvíztűrőtükörfúrógépཤེལ་སྒོ་ཟ་ནས་ང་ན་གི་མ་རེདஇனிதாவதுהאקדמיה ללשון העבריь私はガラスを食べられます";

	public static void dropTable(
		Connection connection,
		String tableName) {

		try {
			executeSql(connection, "drop table " + tableName);
			connection.commit();
		}
		catch (SQLException se) {
			// if the table doesn't exist, we'll get an exception.
			// Ignore it & continue.
			System.out.println(se.getMessage());
		}
	}

	public static void executeSql(
		Connection connection,
		String sql)
		throws SQLException {

		Statement statement = connection.createStatement();
		statement.execute(sql);
	}

	@Test
	public void testFirebird()
		throws Exception {

		dataSource = new BasicDataSource();
		dataSource.setDriverClassName("org.firebirdsql.jdbc.FBDriver");
		dataSource.setDefaultTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
		dataSource.setDefaultAutoCommit(false);
		dataSource.addConnectionProperty("TRANSACTION_READ_COMMITTED", "read_committed,rec_version,nowait");		
		String path = new java.io.File(".").getCanonicalPath() + "/target/test-classes/EMPTYDATABASE.FDB";
		dataSource.setUrl("jdbc:firebirdsql:localhost/3050:" + path + "?charSet=UTF-8");
		dataSource.setUsername("sysdba");
		dataSource.setPassword("masterkey");
		connection = dataSource.getConnection();
				
		dropTable(connection, "BeanTest1");
		dropTable(connection, "BeanTest2");
		
		// Create table from annotation
		@SuppressWarnings("unchecked") SQLExecute<TestBean> x =
			(SQLExecute<TestBean>) SQLExecute.getExecuterByDataSource(dataSource);
		x.createTable(connection, TestBean.class);
		
		if (!x.existsTable(connection, TestBean.class))
				throw new SQLException("Table does not exists");
		
		x.dropTable(connection, TestBean.class);
		
		if (x.existsTable(connection, TestBean.class))
			throw new SQLException("Table exists after drop");
		
		dropTable(connection, "BeanTest1");
		
		// Create table from annotation
		@SuppressWarnings("unchecked") SQLExecute<TestBean2> x2 =
			(SQLExecute<TestBean2>) SQLExecute.getExecuterByDataSource(dataSource);		
		x2.createTable(connection, TestBean2.class);
		
		executeSql(connection, "create table BeanTest1 (" + 
			"ID integer, " + 
			"ID_CUSTOMER integer, "+ 
			"PASSWORD_ANNOTATED varchar(20), " + 
			"FOUR_PART_COLUMN_NAME integer, "+ 
			"DATE_FIELD_WITHOUT_ANNOTATION date," + 
			"DATE_FIELD_WITH_ANNOTATION date," + 
			"BLOB_FIELD blob, "+
			"FLOAT_FIELD NUMERIC(15,2) )");
		connection.commit();

		testExecute();
		dataSource.close();
	}

	@Test
	@Ignore
	public void testMysql()
		throws Exception {

		dataSource = new BasicDataSource();
		dataSource.setDriverClassName("com.mysql.jdbc.Driver");
		dataSource.setDefaultTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
		dataSource.setDefaultAutoCommit(false);
		dataSource.setUrl("jdbc:mysql://localhost:3306/test?useUnicode=yes&characterEncoding=UTF-8");
		dataSource.setUsername("test");
		dataSource.setPassword("test");
		connection = dataSource.getConnection();
				
		dropTable(connection, "BeanTest1");
		dropTable(connection, "BeanTest2");
		connection.commit();
		connection.close();
		connection = dataSource.getConnection();
		
		// Create table from annotation
		@SuppressWarnings("unchecked") SQLExecute<TestBean> x =
			(SQLExecute<TestBean>) SQLExecute.getExecuterByDataSource(dataSource);
		x.createTable(connection, TestBean.class);
		
		if (!x.existsTable(connection, TestBean.class))
				throw new SQLException("Table does not exists");
		
		x.dropTable(connection, TestBean.class);
		
		if (x.existsTable(connection, TestBean.class))
			throw new SQLException("Table exists after drop");
		
		dropTable(connection, "BeanTest1");
		
		// Create table from annotation
		@SuppressWarnings("unchecked") SQLExecute<TestBean2> x2 =
			(SQLExecute<TestBean2>) SQLExecute.getExecuterByDataSource(dataSource);		
		x2.createTable(connection, TestBean2.class);
		
		executeSql(connection, "create table BeanTest1 (" + 
			"ID integer, " + 
			"ID_CUSTOMER integer, "+ 
			"PASSWORD_ANNOTATED varchar(20), " + 
			"FOUR_PART_COLUMN_NAME integer, "+ 
			"DATE_FIELD_WITHOUT_ANNOTATION date," + 
			"DATE_FIELD_WITH_ANNOTATION date," + 
			"BLOB_FIELD LONGTEXT, "+
			"FLOAT_FIELD NUMERIC(15,2) )");
		connection.commit();

		testExecute();
		dataSource.close();
	}

	@Test
	@Ignore
	public void testOracle()
		throws Exception {

		dataSource = new BasicDataSource();
		dataSource.setDriverClassName("oracle.jdbc.driver.OracleDriver");
		dataSource.setDefaultTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
		dataSource.setDefaultAutoCommit(false);
		dataSource.setUrl("jdbc:oracle:thin:@localhost:1521:apollo" );
		dataSource.setUsername("design");
		dataSource.setPassword("mve");
		connection = dataSource.getConnection();
				
		dropTable(connection, "BeanTest1");
		dropTable(connection, "BeanTest2");
		
		// Create table from annotation
		@SuppressWarnings("unchecked") SQLExecute<TestBean> x =
			(SQLExecute<TestBean>) SQLExecute.getExecuterByDataSource(dataSource);
		x.createTable(connection, TestBean.class);
		connection.commit();
		
		if (!x.existsTable(connection, TestBean.class))
				throw new SQLException("Table does not exists");
		
		x.dropTable(connection, TestBean.class);
		
		if (x.existsTable(connection, TestBean.class))
			throw new SQLException("Table exists after drop");
		
		dropTable(connection, "BeanTest1");
		
		// Create table from annotation
		@SuppressWarnings("unchecked") SQLExecute<TestBean2> x2 =
			(SQLExecute<TestBean2>) SQLExecute.getExecuterByDataSource(dataSource);		
		x2.createTable(connection, TestBean2.class);
		
		executeSql(connection, "create table BeanTest1 (" + 
			"ID integer, " + 
			"ID_CUSTOMER integer, "+ 
			"PASSWORD_ANNOTATED varchar(20), " + 
			"FOUR_PART_COLUMN_NAME integer, "+ 
			"DATE_FIELD_WITHOUT_ANNOTATION date," + 
			"DATE_FIELD_WITH_ANNOTATION date," + 
			"BLOB_FIELD clob, "+
			"FLOAT_FIELD NUMERIC(15,2) )");
		connection.commit();

		dateValue = "to_date('2011-01-03','yyyy-mm-dd')";
		blobValue = "ÁRVÍZTŰRŐTÜKÖRFÚRÓGÉPárvíztűrőtükörfúrógép";
		testExecute();
		dataSource.close();
	}

	@Test
	public void testHsqlDb()
		throws Exception {

		// Start HSQL Server
		final Server hsqlServer = new Server();
		hsqlServer.setDatabaseName(0, "testDb");
		hsqlServer.setAddress("127.0.0.1");
		hsqlServer.setDatabasePath(0, "file:./target/test-classes/testDb");
		
		hsqlServer.start();
		dataSource = new BasicDataSource();
		dataSource.setDriverClassName("org.hsqldb.jdbcDriver");
		dataSource.setUrl("jdbc:hsqldb:hsql://localhost/testDb");
		dataSource.setUsername("sa");
		dataSource.setPassword("");
		dataSource.setDefaultAutoCommit(false);
		connection = dataSource.getConnection();
		connection.setAutoCommit(false);
		connection2 = dataSource.getConnection();
		connection2.setAutoCommit(false);		
		executeSql(connection, "SET PROPERTY \"sql.enforce_strict_size\" true");
		
		dropTable(connection, "BeanTest1");
		dropTable(connection, "BeanTest2");
		executeSql(connection, "create table BeanTest1 (" + 
			"ID integer, " + 
			"ID_CUSTOMER integer, " +
			"PASSWORD_ANNOTATED varchar(20), " + 
			"FOUR_PART_COLUMN_NAME integer, " +
			"DATE_FIELD_WITH_ANNOTATION date," + 			
			"DATE_FIELD_WITHOUT_ANNOTATION date," + 
			"BLOB_FIELD longvarchar, " +
			"FLOAT_FIELD numeric (15,2) )");
		executeSql(connection, "create table BeanTest2 (" + 
			"ID integer, " + 
			"ID_CUSTOMER integer, " +
			"PASSWORD_ANNOTATED varchar(20), " + 
			"FOUR_PART_COLUMN_NAME integer, " +
			"DATE_FIELD_WITH_ANNOTATION date," + 			
			"DATE_FIELD_WITHOUT_ANNOTATION date," + 
			"BLOB_FIELD longvarchar, " +
			"FLOAT_FIELD numeric (15,2) )");		
		connection.commit();

		testExecute();
		dataSource.close();
		hsqlServer.stop();
	}

	public void testExecute()
		throws Exception {
		
		for (int i = 0; i < 1000; i++) {
			blob.append(blobValue);
		}
		dateWithoutTime = new SimpleDateFormat("yyyy.MM.dd").parse("2011.01.03");

		@SuppressWarnings("unchecked") SQLExecute<TestBean> x =
			(SQLExecute<TestBean>) SQLExecute.getExecuterByDataSource(dataSource);
		@SuppressWarnings("unchecked") SQLExecute<TestBean2> x2 =
			(SQLExecute<TestBean2>) SQLExecute.getExecuterByDataSource(dataSource);
		@SuppressWarnings("unchecked") SQLExecute<TestBean> xa =
			(SQLExecute<TestBean>) SQLExecute.getExecuterByDataSource(dataSource);		
		@SuppressWarnings("unchecked") SQLExecute<TestBean2> xb =
			(SQLExecute<TestBean2>) SQLExecute.getExecuterByDataSource(dataSource);		
		
		List<TestBean> res = null; 
		List<TestBean2> res2 = null;

		dropTable(connection, "T1");
		x.executeScript(connection, new File("./target/test-classes/test.sql"), "Create");
		x.executeScript(connection, new File("./target/test-classes/test.sql"), "Insert");
		connection.commit();
		connection.close();
		connection = dataSource.getConnection();
		connection.setAutoCommit(false);
		connection2 = dataSource.getConnection();
		connection2.setAutoCommit(false);
				
		x.executeScript(connection, new File("./target/test-classes/test.sql"), "Drop");

		// Execute script with different separator
		x.executeScript(connection, new File("./target/test-classes/test2.sql"), "Test", "[\\\\]");
		
		// Execute script with no separator and no section (single statement)
		x.executeScript(connection, new File("./target/test-classes/test3.sql"));		

		// Insert data with JDBC
		executeSql(
				connection,
				"INSERT INTO BeanTest1(ID, ID_CUSTOMER, PASSWORD_ANNOTATED, FOUR_PART_COLUMN_NAME, DATE_FIELD_WITH_ANNOTATION, DATE_FIELD_WITHOUT_ANNOTATION, BLOB_FIELD, FLOAT_FIELD)" +
					" values (1, 1, 'password', 1, "+dateValue+", "+dateValue+", '"+BLOBTEXT+"', 0.3)");

		// Insert data with bean
		bean = new TestBean();
		bean.setId(2);
		bean.setIdCustomer(2);
		bean.setConfirmationPassword("password");
		bean.setFourPartColumnName(true);
		bean.setDateFieldWithoutAnnotation(date);
		bean.setDateFieldWithAnnotation(dateWithoutTime);
		bean.setBlob(blob.toString());
		bean.setFloatField(new Double(1.0f/3.0f));
		
		x.insertEntity(connection, bean);

		// Query all record
		QueryBuilder builder = new SimpleBeanSQLQueryBuilder(TestBean.class);
		res = x.queryEntities(connection, TestBean.class, builder);

		assertEquals("Entity name", "BeanTest1", AnnotationHelper.getTableName(res.get(0)));
		assertEquals("Resultset size", 2, res.size());

		assertEquals("ID", new Integer(1), res.get(0).getId());
		assertEquals("ID_CUSTOMER", new Integer(1), res.get(0).getId());
		assertEquals("PASSWORD_ANNOTATED", "password", res.get(0).getConfirmationPassword());
		assertEquals("FOUR_PART_COLUMN_NAME", new Boolean(true), res.get(0).getFourPartColumnName());
		assertEquals("BLOB_FIELD", BLOBTEXT, res.get(0).getBlob());
		assertNotNull("DATE_FIELD_WITHOUT_ANNOTATION", res.get(0).getDateFieldWithoutAnnotation());
		assertEquals("DATE_FIELD_WITHOUT_ANNOTATION", dateWithoutTime, res.get(0).getDateFieldWithoutAnnotation());
		assertEquals("DATE_FIELD_WITH_ANNOTATION", dateWithoutTime, res.get(0).getDateFieldWithAnnotation());

		assertEquals("FLOAT_FIELD", 0.3, res.get(0).getFloatField().doubleValue(), 0.0f);
		
		assertEquals("ID", new Integer(2), res.get(1).getId());
		assertEquals("ID_CUSTOMER", new Integer(2), res.get(1).getId());
		assertEquals("PASSWORD_ANNOTATED", "password", res.get(1).getConfirmationPassword());
		assertEquals("FOUR_PART_COLUMN_NAME", new Boolean(true), res.get(1).getFourPartColumnName());
		assertEquals("BLOB_FIELD", blob.toString(), res.get(1).getBlob());
		assertEquals("DATE_FIELD_WITHOUT_ANNOTATION", (Date) null, res.get(1).getDateFieldWithoutAnnotation());
		assertEquals("DATE_FIELD_WITH_ANNOTATION", dateWithoutTime, res.get(0).getDateFieldWithAnnotation());

		assertEquals("FLOAT_FIELD", 0.33, res.get(1).getFloatField().doubleValue(), 0.0f);

		// Updating bean
		res.get(1).setConfirmationPassword("testupdate");
		x.updateEntity(connection, res.get(1));
		// Query all records
		res = x.queryEntities(connection, TestBean.class, builder);
		assertEquals("PASSWORD_ANNOTATED", "testupdate", res.get(1).getConfirmationPassword());

		// Delete bean
		x.deleteEntity(connection, res.get(1));
		// Query all records
		res = x.queryEntities(connection, TestBean.class, builder);
		assertEquals("Resultset size", 1, res.size());
		
		// Generate beans several times
		x.prepareInsertStatement(connection, TestBean.class, new String[] {"id", "idCustomer", "floatField"});
		bean.setId(2);
		x.insertEntityWithPreparedStatement(bean);
		bean.setId(3);
		x.insertEntityWithPreparedStatement(bean);
		bean.setId(4);
		bean.setFloatField(-1.0);
		x.insertEntityWithPreparedStatement(bean);
		bean.setId(5);
		bean.setFloatField(-2.0);
		x.insertEntityWithPreparedStatement(bean);
		
		builder = new SimpleBeanSQLQueryBuilder(TestBean.class);
		builder.setWhere(new AndOperator(new GreaterCriteria<Integer>("a", "id", 1)));		
		res = x.queryEntities(connection, TestBean.class, "a", builder);
		assertEquals("Resultset size", 4, res.size());
		
		connection.commit();
		
		// select two record with parameters
		builder = new SimpleBeanSQLQueryBuilder(TestBean.class);
		builder.setWhere(new AndOperator(new LessCriteria<OperandSource>("c", "float_field", new OperandSource("", ":param11", false))));
		Map<String, Object> params = new HashMap<String, Object>();
		params.put("param11", 0.02);
		builder.setParameters(params);
		res = x.queryEntities(connection, TestBean.class, "c", builder, params);
		assertEquals("Resultset size", 2, res.size());		
		
		// Lock two record before update 
		builder = new SimpleBeanSQLQueryBuilder(TestBean.class);
		builder.setWhere(new AndOperator(new LessCriteria<Double>("c", "float_field", 0.0)));		
		try {
			res = x.lockEntities(connection, TestBean.class, "c", builder);
			assertEquals("Resultset size", 2, res.size());
		}
		catch (QueryBuilderException e) {
			if (x instanceof HSqlDbExecute) {
				assertTrue(true);
			} else {
				assertTrue(false);
			}			
		}
		
		// Test lock conflict
		builder = new SimpleBeanSQLQueryBuilder(TestBean.class);
		builder.setWhere(new AndOperator(new LessCriteria<Double>("c", "float_field", 0.0)));		
		try {
			res = xa.lockEntities(connection2, TestBean.class, "c", builder);
			assertTrue(false);
		}
		catch (QueryBuilderException e) {
			if (xa instanceof HSqlDbExecute) {
				assertTrue(true);
			} else {
				assertTrue(false);
			}			
		}		
		catch (SQLException e) {
			if (xa instanceof HSqlDbExecute) {
				if (e.getErrorCode() == 335544345) {
					assertTrue(true);				
				} else {
					assertTrue(false);
				}
			}
		}
				
		// Update two records (with tabel alias and condition)
		TestBean bean2 = new TestBean();
		bean2.setIdCustomer(-10);
		x.updateEntities(connection, bean2, "c", new String[] {"idCustomer"}, new AndOperator(new LessCriteria<Double>("c", "float_field", 0.0)));
		
		builder = new SimpleBeanSQLQueryBuilder(TestBean.class);
		builder.setWhere(new AndOperator(new EqualCriteria<Integer>("id_customer", -10)));
		res = x.queryEntities(connection, TestBean.class, builder);		
		assertEquals("Resultset size", 2, res.size());	
		
		// Delete 2 records (with table alias and condition)
		if (x instanceof MySqlExecute || x instanceof OracleExecute) {
			// Alies dooes not supported on alias
			x.deleteEntities(connection, TestBean.class, new AndOperator(new BetweenCriteria<Integer>("id", 1, 2)));	
		} else {
			x.deleteEntities(connection, TestBean.class, "b", new AndOperator(new BetweenCriteria<Integer>("b", "id", 1, 2)));
		}
		
		builder = new SimpleBeanSQLQueryBuilder(TestBean.class);
		builder.setOrderBy(new OrderByClause[] {new OrderByClause("id",false), new OrderByClause("id_customer",true)});
		res = x.queryEntities(connection, TestBean.class, builder);
		assertEquals("Resultset size", 3, res.size());
		
		// InsertSelect 1 record (with table alias and condition)
		builder = new SimpleBeanSQLQueryBuilder(TestBean2.class);
		res2 = x2.queryEntities(connection, TestBean2.class, builder);
		assertEquals("Resultset size", 0, res2.size());
		
		x2.insertSelect(connection, 
			TestBean2.class, new String[] {"id", "idCustomer"}, 
			TestBean.class, "", new String[] {"id", "idCustomer"}, new AndOperator(new BetweenCriteria<Integer>("id", 3, 3)), null);		
		
		builder = new SimpleBeanSQLQueryBuilder(TestBean2.class);
		res2 = x2.queryEntities(connection, TestBean2.class, builder);
		assertEquals("Resultset size", 1, res2.size());
		
		// InsertSelect 2 record (with table alias and condition and parameters)
		Map<String, Object> params2 = new HashMap<String, Object>();
		params2.put("id", 3);
		
		x2.insertSelect(connection, 
			TestBean2.class, new String[] {"id", "idCustomer"}, 
			TestBean.class, "", new String[] {"id", "idCustomer"},
			new AndOperator(new GreaterCriteria<OperandSource>("id", new OperandSource("", ":id", false))),
			params2);		
		
		builder = new SimpleBeanSQLQueryBuilder(TestBean2.class);
		res2 = x2.queryEntities(connection, TestBean2.class, builder);
		assertEquals("Resultset size", 3, res2.size());		
		
		
		
		connection.commit();
		
		
		
		// Lock one record	
		try {
			x2.lockEntity(connection, res2.get(0));
		}
		catch (QueryBuilderException e) {
			if (x2 instanceof HSqlDbExecute) {
				assertTrue(true);
			} else {
				assertTrue(false);
			}			
		}
		
		// Test one conflict
		try {
			xb.lockEntity(connection2, res2.get(0));
			assertTrue(false);
		}
		catch (QueryBuilderException e) {
			if (xb instanceof HSqlDbExecute) {
				assertTrue(true);
			} else {
				assertTrue(false);
			}			
		}		
		catch (SQLException e) {
			if (xb instanceof HSqlDbExecute) {
				if (e.getErrorCode() == 335544345) {
					assertTrue(true);				
				} else {
					assertTrue(false);
				}
			}
		}		
						
		// Delete all
		x.deleteEntities(connection, TestBean.class, null);
		builder = new SimpleBeanSQLQueryBuilder(TestBean.class);
		
		res = x.queryEntities(connection, TestBean.class, builder);
		assertEquals("Resultset size", 0, res.size());


		connection.commit();
		connection.close();
		
		connection2.commit();
		connection2.close();		
	}
}
