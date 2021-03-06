package org.liveSense.api.sql;

import java.util.List;

import org.liveSense.api.sql.exceptions.SQLException;
import org.liveSense.misc.queryBuilder.clauses.DefaultLimitClause;
import org.liveSense.misc.queryBuilder.clauses.DefaultOrderByClause;
import org.liveSense.misc.queryBuilder.domains.LimitClause;
import org.liveSense.misc.queryBuilder.domains.OrderByClause;
import org.liveSense.misc.queryBuilder.exceptions.QueryBuilderException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OracleExecute<T> extends SQLExecute<T> {

	@SuppressWarnings("unused")
	private static final Logger log = LoggerFactory.getLogger(OracleExecute.class);
	private int maxSize = -1;
	
	public OracleExecute(int maxSize) {
		this.maxSize = maxSize;
	}
	
	
	private ClauseHelper makeSubSelective(ClauseHelper helper, String tableAlias) {
		helper.setQuery("SELECT * FROM ("+helper.getQuery()+") "+tableAlias);
		helper.setSubSelect(true);
		return helper;
	}
	
	/**
	 * {@inheritDoc}
	 * @throws QueryBuilderException 
	 * @see {@link SQLExecute#addWhereClause(SQLExecute.ClauseHelper)}
	 */
	public ClauseHelper addWhereClause(ClauseHelper helper) throws SQLException, QueryBuilderException {
		return addWhereClause(helper, "");
	}

	/**
	 * {@inheritDoc}
	 * @throws QueryBuilderException 
	 * @param String tableAlias
	 * @see {@link SQLExecute#addWhereClause(SQLExecute.ClauseHelper)}
	 */
	public ClauseHelper addWhereClause(ClauseHelper helper, String tableAlias) throws SQLException, QueryBuilderException {
		String whereClause = builder.buildWhere(helper.getClazz(), builder.getWhere());
		if (!"".equals(whereClause)) {			
			if (!helper.getSubSelect()) {
				 makeSubSelective(helper, tableAlias);
			}
			helper.setWhereClause(true);
			helper.setQuery(helper.getQuery()+ " WHERE "+whereClause);
		}
		return helper;
	}
	
	/**
	 * {@inheritDoc}
	 * @throws SQLException
	 * @see {@link SQLExecute#addLimitClause(SQLExecute.ClauseHelper)}
	 */
	public ClauseHelper addLimitClause(ClauseHelper helper) throws SQLException {
		return addLimitClause(helper, "");
	}
	
	/**
	 * {@inheritDoc}
	 * @throws SQLException
	 * @param String tableAlias 
	 * @see {@link SQLExecute#addLimitClause(SQLExecute.ClauseHelper)}
	 */
	public ClauseHelper addLimitClause(ClauseHelper helper, String tableAlias) throws SQLException {
		LimitClause limit = builder.getLimit();
		if (limit == null) 
			limit = new DefaultLimitClause(maxSize, 0);
		
		if (maxSize > 0 && limit.getLimit() > maxSize && limit.getLimit() > 0) {
			limit.setLimit(maxSize);			
		}
		
		int l = limit.getLimit();
		int o = limit.getOffset();
		if (l < 1 && o < 1)
			return helper;
			
		helper.setLimitClause(true);
		if (!helper.getSubSelect()) {
			makeSubSelective(helper, tableAlias);
		}
		
		if (helper.getWhereClause()) {
			helper.setQuery(helper.getQuery()+" AND ");
		} else {
			helper.setQuery(helper.getQuery()+" WHERE ");			
		}
		if (l > 0 && o > 0) {
			helper.setQuery(helper.getQuery()+ " ROWNUM >= "+o+1+" AND ROWNUM <= "+l+o);
		} else if (l > 0 && o < 1) {
			helper.setQuery(helper.getQuery()+ " ROWNUM <= "+l);			
		} else if (l < 1 || o > 0){
			helper.setQuery(helper.getQuery()+ " ROWNUM >= "+o+1);			
		}

		return helper;
	}
	
	/**
	 * {@inheritDoc}
	 * @throws SQLException 
	 * @see {@link SQLExecute#addOrderByClause(SQLExecute.ClauseHelper)}
	 */
	public ClauseHelper addOrderByClause(ClauseHelper helper) throws SQLException {
		return addOrderByClause(helper,"");
	}
	
	/**
	 * {@inheritDoc}
	 * @throws SQLException 
	 * @see {@link SQLExecute#addOrderByClause(SQLExecute.ClauseHelper)}
	 */
	public ClauseHelper addOrderByClause(ClauseHelper helper, String tableAlias) throws SQLException {	
		List<OrderByClause> orderby = builder.getOrderBy();
		if ((orderby == null) || (orderby.size() == 0))
			return helper;
		
		helper.setOrderByClause(true);
		if (!helper.getSubSelect()) {
			makeSubSelective(helper, tableAlias);
		}		
						
		boolean first = true;
		StringBuffer sb = new StringBuffer(" ORDER BY ");
		for (OrderByClause orderByClause : orderby) {
			if (first) { first = false;	} 
			else { sb.append(","); }
			sb.append(orderByClause.getFieldName());
			if (orderByClause.getSortDesc() != null) { 
				sb.append(orderByClause.getSortDesc() ? " DESC" : " ASC");
			}
		}
		helper.setQuery(helper.getQuery() + sb.toString());		
		
		return helper;
	}

	
	@SuppressWarnings("rawtypes")
	@Override
	public String getSelectQuery(Class clazz) throws SQLException, QueryBuilderException {
		return getSelectQuery(clazz, "");
	}
	
	@SuppressWarnings("rawtypes")
	@Override
	public String getSelectQuery(Class clazz, String tableAlias) throws SQLException, QueryBuilderException {
		ClauseHelper helper = new ClauseHelper(clazz, builder.getQuery(tableAlias), false);
		ClauseHelper cls = 
			addOrderByClause(addLimitClause(addWhereClause(helper,tableAlias),tableAlias),tableAlias);
		return cls.getQuery();
	}	
	
	@SuppressWarnings("rawtypes")
	@Override
	public String getLockQuery(Class clazz) throws SQLException, QueryBuilderException {
		return getLockQuery(clazz, "");
	}
	
	@SuppressWarnings("rawtypes")
	@Override
	public String getLockQuery(Class clazz, String tableAlias) throws SQLException, QueryBuilderException {
		ClauseHelper helper = new ClauseHelper(clazz, builder.getQuery(tableAlias), true);
		ClauseHelper cls = addOrderByClause(addLimitClause(addWhereClause(helper, tableAlias),tableAlias),tableAlias);
		return cls.getQuery() + " FOR UPDATE NOWAIT";
	}
	
	@Override
	public String getBlobName(){
		return "CLOB";
	}
	
}
