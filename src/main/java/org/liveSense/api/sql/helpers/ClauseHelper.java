package org.liveSense.api.sql.helpers;


public class ClauseHelper {

	
	//FIELDS
	@SuppressWarnings("rawtypes")
	private Class clazz;
	private String query;
	private Boolean subSelect;
	
	private Boolean whereClause = false;;
	private Boolean orderByClause = false;
	private Boolean limitClause = false;
	
	
	//CONSTRUCTOR
	@SuppressWarnings("rawtypes")
	public ClauseHelper(Class clazz, String query, Boolean subSelect) {
		this.clazz = clazz;
		this.query = query;
		this.subSelect = subSelect;
	}
	
	
	//GETTERS AND SETTERS
	public Boolean getWhereClause() {
		return whereClause;
	}

	public void setWhereClause(Boolean whereClause) {
		this.whereClause = whereClause;
	}

	public Boolean getOrderByClause() {
		return orderByClause;
	}

	public void setOrderByClause(Boolean orderByClause) {
		this.orderByClause = orderByClause;
	}

	public Boolean getLimitClause() {
		return limitClause;
	}

	public void setLimitClause(Boolean limitClause) {
		this.limitClause = limitClause;
	}

	@SuppressWarnings("rawtypes")
	public Class getClazz() {
		return clazz;
	}
	
	@SuppressWarnings("rawtypes")
	public void setClazz(
		Class clazz) {
		this.clazz = clazz;
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