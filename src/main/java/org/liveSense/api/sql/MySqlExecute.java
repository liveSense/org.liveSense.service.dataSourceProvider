package org.liveSense.api.sql;

import org.liveSense.api.sql.exceptions.SQLException;
import org.liveSense.misc.queryBuilder.clauses.LimitClause;
import org.liveSense.misc.queryBuilder.exceptions.QueryBuilderException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MySqlExecute<T> extends SQLExecute<T> {

	private static final Logger log = LoggerFactory.getLogger(MySqlExecute.class);
	private int maxSize = -1;
	
	public MySqlExecute(int maxSize) {
		this.maxSize = maxSize;
	}
	
	private ClauseHelper makeSubSelective(ClauseHelper helper) {
		helper.setQuery("SELECT * FROM ("+helper.getQuery()+") stm");
		helper.setSubSelect(true);
		return helper;
	}
	
	/**
	 * {@inheritDoc}
	 * @throws QueryBuilderException 
	 * @see {@link SQLExecute#addWhereClause(SQLExecute.ClauseHelper)}
	 */
	public ClauseHelper addWhereClause(ClauseHelper helper) throws SQLException, QueryBuilderException {
		String whereClause = builder.buildParameters();
		if (!"".equals(whereClause)) {
			if (!helper.getSubSelect()) {
				 makeSubSelective(helper);
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
		LimitClause limit = builder.getLimit();
		if (limit == null) 
			limit = new LimitClause(maxSize, 0);
		
		if (maxSize > 0 && limit.getLimit() > maxSize && limit.getLimit() > 0) {
			limit.setLimit(maxSize);			
		}
		if (!helper.getSubSelect() && (
				limit.getLimit() > 0  || limit.getOffset() > 0)) {
			if (!helper.getSubSelect()) {
				makeSubSelective(helper);
			}
		}
		if (limit.getLimit() > 0) {
			helper.setQuery(helper.getQuery()+ " LIMIT "+limit.getLimit());
		}
		if (limit.getOffset() > 0) {
			helper.setQuery(helper.getQuery()+ " OFFSET "+limit.getOffset());
		}

		return helper;
	}
	
	/**
	 * {@inheritDoc}
	 * @throws SQLException 
	 * @see {@link SQLExecute#addOrderByClause(SQLExecute.ClauseHelper)}
	 */
	public ClauseHelper addOrderByClause(ClauseHelper helper) throws SQLException {
		return helper;
	}


	
	@Override
	public String getSelectQuery() throws SQLException, QueryBuilderException {
		ClauseHelper cls = addLimitClause(addOrderByClause(addWhereClause(new ClauseHelper(builder.getQuery(), false))));
		return cls.getQuery();
	}
	
}
