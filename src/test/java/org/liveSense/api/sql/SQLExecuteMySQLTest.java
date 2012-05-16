package org.liveSense.api.sql;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;

import org.apache.commons.dbcp.BasicDataSource;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;

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
		
	
}
