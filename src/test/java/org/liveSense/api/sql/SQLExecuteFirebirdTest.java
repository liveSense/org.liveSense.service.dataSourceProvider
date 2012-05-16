package org.liveSense.api.sql;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;

import org.apache.commons.dbcp.BasicDataSource;
import org.junit.After;
import org.junit.Before;


public class SQLExecuteFirebirdTest
	extends SQLExecuteBaseTest {
	
	
	//TEST - init/fina
	@Before
	public void before()  {
		try {
			connect();
			init("blob");
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
	@Override
	protected void createTestProcedure()
		throws Exception {
		
		String sql =
			"CREATE OR ALTER PROCEDURE testproc(\n"+
			"    param1 VARCHAR(20),\n"+
			"    param2 INTEGER)\n"+
			"RETURNS(\n" +
			"    param3 NUMERIC(15,2))\n"+
			"AS\n"+
			"BEGIN\n" +
			"    param3 = CAST(param1 AS NUMERIC(15,2)) + param2;\n"+
			"END";
		executeSql(connection, sql);
		connection.commit();
	}

	
	//METHODS - private
	private void connect()
		throws IOException, SQLException {
	
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
		connection2 = dataSource.getConnection();
		
		connection.setAutoCommit(false);
		connection2.setAutoCommit(false);
	}
	
	
}
