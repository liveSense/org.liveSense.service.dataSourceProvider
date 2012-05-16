package org.liveSense.api.sql;

import java.io.IOException;
import java.sql.SQLException;

import org.apache.commons.dbcp.BasicDataSource;
import org.hsqldb.Server;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.liveSense.api.sql.SQLExecuteBaseTest.DatabaseIsConnected;

import com.googlecode.junit.ext.RunIf;


public class SQLExecuteHSQLTest
	extends SQLExecuteBaseTest {
	
	
	//FIELDS
	private static Server hsqlServer;
	
	//TEST - init/fina
	@BeforeClass
	public static void beforeClass() throws Exception {
		
		hsqlServer = new Server();
		hsqlServer.setDatabaseName(0, "testDb");
		hsqlServer.setAddress("127.0.0.1");
		hsqlServer.setDatabasePath(0, "file:./target/test-classes/testDb");
		
		hsqlServer.start();
		boolean running = false;
		long serverTimeout = 10000;
		long startTime = System.currentTimeMillis();
		while (!running) {
			if (System.currentTimeMillis() > startTime+serverTimeout)
				throw new Exception("HSQL Server timed out");
			try {
				hsqlServer.checkRunning(true);
				running = true;
			} catch (Exception e) {
			}
		}
	}
	
	@AfterClass
	public static void afterClass() {

		hsqlServer.stop();		
	}
	
	@Before
	public void before() 
		throws Exception {
		
		connect();
		init("longvarchar");
	}
	
	@After
	public void after() 
		throws SQLException {
		
		disconnect();
	}
	
	//METHODS - abstract
	@Override
	protected void createTestProcedure()
		throws Exception {
		
		//VERY unique storedproc support		
	}

	
	//METHODS - private
	private Server connect() 
		throws IOException, SQLException {
		
		dataSource = new BasicDataSource();
		dataSource.setDriverClassName("org.hsqldb.jdbcDriver");
		dataSource.setUrl("jdbc:hsqldb:hsql://localhost/testDb");
		dataSource.setUsername("sa");
		dataSource.setPassword("");
		dataSource.setDefaultAutoCommit(false);
		connection = dataSource.getConnection();		
		connection2 = dataSource.getConnection();
		
		connection.setAutoCommit(false);
		connection2.setAutoCommit(false);
		
		executeSql(connection, "SET PROPERTY \"sql.enforce_strict_size\" true");
		
		return hsqlServer;
	}
	
	@Override
	protected void disconnect() throws SQLException {
		
		super.disconnect();
	}
	
	
	//TESTS 
	//record level lock unsupported by HSQL
	@Test
	@RunIf(DatabaseIsConnected.class)
	public void testPrepareLockMulti() 
		throws Exception {
	}
	
	@Test
	@RunIf(DatabaseIsConnected.class)
	public void testRunPreparedLockMulti() 
		throws Exception {
	}
	
	@Test
	@RunIf(DatabaseIsConnected.class)
	public void testRunLockMulti() 
		throws Exception {
	}
	
	@Test
	@RunIf(DatabaseIsConnected.class)
	public void testPrepareLockSingle() 
		throws Exception {
	}
	
	@Test
	@RunIf(DatabaseIsConnected.class)
	public void testRunPreparedLockSingle() 
		throws Exception {
	}
	
	@Test
	@RunIf(DatabaseIsConnected.class)
	public void testRunLockSingle() 
		throws Exception {
	}
	
	//VERY unique storedproc support by HSQL
	@Test
	@RunIf(DatabaseIsConnected.class)
	public void testPrepareExecuteProcedure() 
		throws Exception {
	}
	
	@Test
	@RunIf(DatabaseIsConnected.class)
	public void testRunPreparedExecuteProcedure() 
		throws Exception {
	}
	
	@Test
	@RunIf(DatabaseIsConnected.class)
	public void testRunExecuteProcedure() 
		throws Exception {
	}
	

}
