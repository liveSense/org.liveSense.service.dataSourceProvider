package org.liveSense.api.sql.helper;

import static org.junit.Assert.assertTrue;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Test;
import org.liveSense.api.sql.NamedParameterProcessor;
import org.liveSense.api.sql.exceptions.SQLException;


public class NamedParameterProcessorTest {
	
	private static final String SQL = "SELECT * FROM dual WHERE a = :a1 AND b = :b2 AND a + b = :a1 + :b3 AND c = :long_valid_param_name"; 
	private static final String SQLPROCESSED = "SELECT * FROM dual WHERE a = ? AND b = ? AND a + b = ? + ? AND c = ?";
	
	
	@Test
	public void constructorOK() 
		throws SQLException {
		
		//tested method
		NamedParameterProcessor npp = new NamedParameterProcessor(SQL);
		
		//tests
		assertTrue(npp.getSqlStatement().equals(SQLPROCESSED));
		
		assertTrue(npp.getParameters().get("a1").size() == 2);
		assertTrue(npp.getParameters().get("a1").get(0) == 1);
		assertTrue(npp.getParameters().get("a1").get(1) == 3);
		
		assertTrue(npp.getParameters().get("b2").size() == 1);
		assertTrue(npp.getParameters().get("b2").get(0) == 2);
		
		assertTrue(npp.getParameters().get("b3").size() == 1);
		assertTrue(npp.getParameters().get("b3").get(0) == 4);
		
		assertTrue(npp.getParameters().get("long_valid_param_name").size() == 1);
		assertTrue(npp.getParameters().get("long_valid_param_name").get(0) == 5);
	}
	
	@Test
	public void getSQLParametersOK()
		throws SQLException {
		
		//prepare
		NamedParameterProcessor npp = new NamedParameterProcessor(SQL);
		Map<String, Object> params = new HashMap<String, Object>();
		params.put("a1", new Integer(1));
		params.put("b2", new Integer(2));
		params.put("b3", new Integer(3));
		params.put("long_valid_param_name", "D'oh!");
		
		//tested method
		List<Object> sqlParams = npp.getSQLParameters(params);
		
		//tests
		assertTrue(sqlParams.size() == 5);
		assertTrue((Integer)sqlParams.get(0) == 1);
		assertTrue((Integer)sqlParams.get(1) == 2);
		assertTrue((Integer)sqlParams.get(2) == 1);
		assertTrue((Integer)sqlParams.get(3) == 3);
		assertTrue(((String)sqlParams.get(4)).equals("D'oh!"));		
	}

}
