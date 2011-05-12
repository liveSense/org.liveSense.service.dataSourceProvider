package org.liveSense.api.sql;

import java.util.List;

import org.liveSense.api.sql.exceptions.SQLException;
import org.liveSense.misc.queryBuilder.clauses.LimitClause;
import org.liveSense.misc.queryBuilder.clauses.OrderByClause;
import org.liveSense.misc.queryBuilder.exceptions.QueryBuilderException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MySqlExecute<T> extends SQLExecute<T> {

	@SuppressWarnings("unused")
	private static final Logger log = LoggerFactory.getLogger(MySqlExecute.class);
	private int maxSize = -1;
	
	public MySqlExecute(int maxSize) {
		this.maxSize = maxSize;
	}
	
	private ClauseHelper makeSubSelective(ClauseHelper helper, String tableAlias) {
		helper.setQuery("SELECT * FROM ("+helper.getQuery()+") ");
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
		String whereClause = builder.buildParameters();
		if (!"".equals(whereClause)) {		
			if (!helper.getSubSelect()) {
				 makeSubSelective(helper, tableAlias);
			}
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
			limit = new LimitClause(maxSize, 0);
		
		if (maxSize > 0 && limit.getLimit() > maxSize && limit.getLimit() > 0) {
			limit.setLimit(maxSize);			
		}
		
		int l = limit.getLimit();
		int o = limit.getOffset();
		if (l < 1 && o < 1)
			return helper;
					
		if (!helper.getSubSelect()) {
			makeSubSelective(helper, tableAlias);
		}
		
		if (limit.getLimit() > 0) {
			helper.setQuery(helper.getQuery()+ " LIMIT "+l);
		}
		if (limit.getOffset() > 0) {
			helper.setQuery(helper.getQuery()+ " OFFSET "+o);
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
		
		if (!helper.getSubSelect()) {
			makeSubSelective(helper, tableAlias);
		}		
						
		boolean first = true;
		StringBuffer sb = new StringBuffer("ORDER BY ");
		for (OrderByClause orderByClause : orderby) {
			if (first) { first = false;	} 
			else { sb.append(","); }
			sb.append(orderByClause.getFieldName());
			sb.append(orderByClause.getSortDesc() ? " DESC" : " ASC");			
		}
		helper.setQuery(helper.getQuery() + sb.toString());		
		
		return helper;
	}


	@Override
	public String getSelectQuery() throws SQLException, QueryBuilderException {
		return getSelectQuery("");
	}
	
	@Override
	public String getSelectQuery(String tableAlias) throws SQLException, QueryBuilderException {
		ClauseHelper helper = new ClauseHelper(builder.getQuery(tableAlias), false);
		ClauseHelper cls = 
			addLimitClause(addOrderByClause(addWhereClause(helper,tableAlias),tableAlias),tableAlias);
		return cls.getQuery();
	}	
	
	@Override
	public String getLockQuery() throws SQLException, QueryBuilderException {
		return getLockQuery("");
	}
	
	@Override
	public String getLockQuery(String tableAlias) throws SQLException, QueryBuilderException {
		ClauseHelper helper = new ClauseHelper(builder.getQuery(tableAlias), true);
		ClauseHelper cls = addLimitClause(addOrderByClause(addWhereClause(helper, tableAlias),tableAlias),tableAlias);
		return cls.getQuery() + " FOR UPDATE";
	}		
	
}
