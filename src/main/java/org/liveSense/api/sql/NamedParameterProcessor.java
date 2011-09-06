package org.liveSense.api.sql;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.lang.StringUtils;
import org.liveSense.api.sql.exceptions.SQLException;


public class NamedParameterProcessor {
	
	
	//CONSTS
	private static final String UNKNOWN_PARAMETER = "Unknown parameter";
	
	
	//FIELDS
	private Map<String, List<Integer>> parameters = new HashMap<String, List<Integer>>();
	private String sqlStatement;	

	
	//GETTERS AND SETTERS
	public Map<String, List<Integer>> getParameters() {	
		return parameters;
	}
	
	public String getSqlStatement() {	
		return sqlStatement;
	}	
	
	
	//CONSTRUCTORS
	public NamedParameterProcessor(String sql) 
		throws SQLException {
			
		int paramCount = 0;
		int ps = sql.indexOf(':');
		while (ps != -1) {
			//check whether we found it in a string literal
			if (StringUtils.countMatches(sql.substring(1, ps), "'") % 2 == 0) {
				paramCount++;
				
				//get parameter name
				StringBuffer sb = new StringBuffer();
				ps++;
				while ((ps + 1 <= sql.length()) && (sql.substring(ps, ps + 1).matches("[a-zA-Z0-9_]+")))  {
					sb.append(sql.substring(ps, ps + 1));
					ps++;
				}
				//store position
				String paramName = sb.toString();
				List<Integer> list = parameters.get(paramName);
				if (list == null) {
					list = new ArrayList<Integer>();
					parameters.put(paramName, list);					
				}				
				list.add(paramCount);
				StringBuffer sb2 = new StringBuffer(sql);
				sb2.delete(ps - paramName.length() - 1, ps);
				sb2.insert(ps - paramName.length() - 1, "?");
				sql = sb2.toString();
				ps = ps - paramName.length();
			}
			ps = sql.indexOf(':', ps + 1);
		}
		sqlStatement = sql;
	}
	
	
	//METHODS
	public List<Object> getSQLParameters(Map<String, Object> params) 
		throws SQLException {
		
		ArrayList<Object> list = new ArrayList<Object>();
		
		if (params == null) 
			return list;
		
		for (Entry<String, Object> paramsEntry : params.entrySet()) {
			List<Integer> positions = null;
			//case insensitive "get"
			for (Entry<String, List<Integer>> parametersEntry : parameters.entrySet()) {
				if (parametersEntry.getKey().toLowerCase().equals(paramsEntry.getKey().toLowerCase())) {
					positions = parametersEntry.getValue();				
				}				
			}
						
			if (positions == null)
				throw new SQLException(UNKNOWN_PARAMETER + " : "+paramsEntry.getKey());
			
			for (Integer pos : positions) {
				while (list.size() < pos) list.add(null);
				list.set(pos - 1, paramsEntry.getValue());
			}			
		}
		return list;
	}	

}
