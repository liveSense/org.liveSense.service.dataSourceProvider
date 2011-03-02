package org.liveSense.api.sql;

import static org.junit.Assert.*;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

import org.apache.commons.dbcp.BasicDataSource;
import org.hsqldb.Server;
import org.hsqldb.persist.HsqlProperties;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.liveSense.api.beanprocessors.TestBean;
import org.liveSense.misc.queryBuilder.QueryBuilder;
import org.liveSense.misc.queryBuilder.SimpleSQLQueryBuilder;

public class HSQLJdbcExecuteTest {

	private static final Server hsqlServer = new Server();
	private static final BasicDataSource dataSource = new BasicDataSource();
	private Connection connection;
	
	public static void setupTables(Connection connection) throws SQLException {

	    dropTable(connection, "BeanTest1");
	    executeSql(connection,
	    		"create table BeanTest1 ("
	    		+"ID integer, "
	    		+"ID_CUSTOMER integer, "
	    		+"PASSWORD_ANNOTATED varchar(20), "
	    		+"FOUR_PART_COLUMN_NAME integer, "
	    		+"DATE_FIELD_WITHOUT_ANNOTATION date)");
	    connection.commit();
	    		
	}

	public static void dropTable(Connection connection, String tableName) {
	    try {
	        executeSql(connection, "drop table " + tableName);
	        connection.commit();
	    } catch (SQLException se) {
	        // if the table doesn't exist, we'll get an exception.
	        // Ignore it & continue.
	    }
	}

	private static void dropSequence(Connection connection, String sequenceName) {
	    try {
	        executeSql(connection, "drop sequence " + sequenceName);
	        connection.commit();
	    } catch (SQLException se) {
	        // ignore exception
	    }
	}

	public static void executeSql(Connection connection, String sql) throws SQLException {
	    Statement statement = connection.createStatement();
	    statement.execute(sql);
	}

	
	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		// Start HSQL Server
		hsqlServer.setDatabaseName(0, "testDb");
		hsqlServer.setAddress("127.0.0.1");
		hsqlServer.setDatabasePath(0, "file:./target/db/testDb");
		HsqlProperties props = new HsqlProperties();
		hsqlServer.start();
		dataSource.setDriverClassName("org.hsqldb.jdbcDriver");
		dataSource.setUrl("jdbc:hsqldb:hsql://localhost/testDb");
		//dataSource.setValidationQuery("SELECT 1 FROM DUAL");
		dataSource.setUsername("sa");
		dataSource.setPassword("");
		dataSource.setDefaultAutoCommit(false);
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
		hsqlServer.stop();
	}

	@Before
	public void setUp() throws Exception {
		connection = dataSource.getConnection();
		setupTables(connection);
		executeSql(connection, "INSERT INTO BeanTest1(ID, ID_CUSTOMER, PASSWORD_ANNOTATED, FOUR_PART_COLUMN_NAME, DATE_FIELD_WITHOUT_ANNOTATION)"
				+" values (1, 1, 'password', 1, CURRENT_DATE)");
	}
	
	@After
	public void tearDown() throws Exception {
		connection.commit();
		connection.close();
	}

	@Test
	public void testExecute() throws Exception {
		QueryBuilder builder = new SimpleSQLQueryBuilder("SELECT * FROM BeanTest1");
		@SuppressWarnings("unchecked")
		SQLExecute<TestBean> x = (SQLExecute<TestBean>) SQLExecute.getExecuterByDataSource(dataSource);
		List<TestBean> res = x.queryEntities(connection, TestBean.class, builder);
		assertEquals("Resultset size", 1, res.size());
		assertEquals("ID", new Integer(1), res.get(0).getId());
		assertEquals("ID_CUSTOMER", new Integer(1), res.get(0).getId());
		assertEquals("PASSWORD_ANNOTATED", "password", res.get(0).getConfirmationPassword());
		assertEquals("FOUR_PART_COLUMN_NAME", new Boolean(true) , res.get(0).getFourPartColumnName());
		assertNotNull("DATE_FIELD_WITHOUT_ANNOTATION", res.get(0).getDateFieldWithoutAnnotation());
	}
}
