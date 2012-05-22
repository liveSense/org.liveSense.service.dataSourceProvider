package org.liveSense.api.sql;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;

import junit.framework.Assert;

import org.apache.commons.dbcp.BasicDataSource;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.liveSense.api.sql.SQLExecuteBaseTest.DatabaseIsConnected;

import com.googlecode.junit.ext.RunIf;

public class SQLExecuteMySQLTest
	extends SQLExecuteBaseTest {
	
	
	//TEST - init/fina
	@Before
	public void before() 
		throws Exception {
		
		try {
			connect();
			init("longtext");
		} catch (Exception e) {
			connection = null;
			org.junit.Assume.assumeNoException(e);
		}
	}
	
	@After
	public void after() 
		throws SQLException {
		
		if (connection != null) disconnect();
	}
	
	//METHODS - abstract
	protected void createTestProcedure() 
		throws Exception {
		
		throw new Exception("Not implemented!");
	}

	
	//METHODS - private
	private void connect() 
		throws IOException, SQLException {
		
		dataSource = new BasicDataSource();
		dataSource.setDriverClassName("com.mysql.jdbc.Driver");
		dataSource.setDefaultTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
		dataSource.setDefaultAutoCommit(false);
		dataSource.setUrl("jdbc:mysql://localhost:3306/test?useUnicode=yes&characterEncoding=UTF-8");
		dataSource.setUsername("test");
		dataSource.setPassword("test");
		connection = dataSource.getConnection();
		connection2 = dataSource.getConnection();
	
		connection.setAutoCommit(false);
		connection2.setAutoCommit(false);
	}
		
	@Override
	@Test
	@RunIf(DatabaseIsConnected.class)
	public void testPrepareUpdateMulti() throws Exception {
	}

	@Override
	@Test
	@RunIf(DatabaseIsConnected.class)
	public void testPrepareQueryMulti() throws Exception {
	}

	@Override
	@Test
	@RunIf(DatabaseIsConnected.class)
	public void testRunPreparedQueryMulti() throws Exception {
	}
	
	@Override
	@Test
	@RunIf(DatabaseIsConnected.class)
	public void testRunQueryMulti() throws Exception {
	}
	
	@Override
	@Test
	@RunIf(DatabaseIsConnected.class)
	public void testPrepareQuerySingle() throws Exception {
	}
	
	@Override
	@Test
	@RunIf(DatabaseIsConnected.class)
	public void testRunPreparedQuerySingle() throws Exception {
	}
	
	@Override
	@Test
	@RunIf(DatabaseIsConnected.class)
	public void testRunQuerySingle() throws Exception {
	}

	@Override
	@Test
	@RunIf(DatabaseIsConnected.class)
	public void testPrepareLockMulti() throws Exception {
	}
	
	@Override
	@Test
	@RunIf(DatabaseIsConnected.class)
	public void testRunPreparedLockMulti() throws Exception {
	}

	@Override
	@Test
	@RunIf(DatabaseIsConnected.class)
	public void testRunLockMulti() throws Exception {
	}

	@Override
	@Test
	@RunIf(DatabaseIsConnected.class)
	public void testPrepareLockSingle() throws Exception {
	}
	
	@Override
	@Test
	@RunIf(DatabaseIsConnected.class)
	public void testRunPreparedLockSingle() throws Exception {
	}

	@Override
	@Test
	@RunIf(DatabaseIsConnected.class)
	public void testRunLockSingle() throws Exception {
	}
	
	@Override
	@Test
	@RunIf(DatabaseIsConnected.class)
	public void testPrepareInsertSelect() throws Exception {
	}

	@Override
	@Test
	@RunIf(DatabaseIsConnected.class)
	public void testPrepareInsertSelect2() throws Exception {
	}

	@Override
	@Test
	@RunIf(DatabaseIsConnected.class)
	public void testRunPreparedInsertSelect() throws Exception {
	}
	
	@Override
	@Test
	@RunIf(DatabaseIsConnected.class)
	public void testRunInsertSelect() throws Exception {
	}
	
	@Override
	@Test
	@RunIf(DatabaseIsConnected.class)
	public void testRunPreparedDeleteMulti() throws Exception {
	}

	@Override
	@Test
	@RunIf(DatabaseIsConnected.class)
	public void testRunDeleteMulti() throws Exception {
	}
	
	@Override
	@Test
	@RunIf(DatabaseIsConnected.class)
	public void testPrepareExecuteProcedure() throws Exception {
	}
	
	@Override
	@Test
	@RunIf(DatabaseIsConnected.class)
	public void testRunPreparedExecuteProcedure() throws Exception {
	}
	
	@Override
	@Test
	@RunIf(DatabaseIsConnected.class)
	public void testRunExecuteProcedure() throws Exception {
	}
	
}