package org.liveSense.api.sql;

import java.util.List;

import org.liveSense.api.sql.exceptions.SQLException;
import org.liveSense.misc.queryBuilder.QueryBuilder;
import org.liveSense.misc.queryBuilder.clauses.LimitClause;
import org.liveSense.misc.queryBuilder.clauses.OrderByClause;
import org.liveSense.misc.queryBuilder.domains.Operator;


public class SimpleBeanSQLQueryBuilder  extends QueryBuilder {

	String statement;
	
	public SimpleBeanSQLQueryBuilder(Class<?> clazz) throws SQLException {
		this(clazz, null, null, null);
	}

	public SimpleBeanSQLQueryBuilder(Class<?> clazz, Operator parameters) throws SQLException {
		this(clazz, parameters, null, null);
	}

	public SimpleBeanSQLQueryBuilder(Class<?> clazz, LimitClause limit) throws SQLException {
		this(clazz, null, limit, null);
	}

	public SimpleBeanSQLQueryBuilder(Class<?> clazz, Operator parameters, LimitClause limit) throws SQLException {
		this(clazz, parameters, limit, null);
	}
		
	public SimpleBeanSQLQueryBuilder(Class<?> clazz, Operator parameters, LimitClause limit, List<OrderByClause> orderBy) throws SQLException {
		// Search for entity annotation
		String tableName = AnnotationHelper.getTableName(clazz);
		if (tableName == null || "".equals(tableName)) throw new SQLException("Entity annotation does not found");
		this.statement = "SELECT * FROM "+tableName;
		setLimit(limit);
		setOrderBy(orderBy);
		setWhere(parameters);
	}

	@Override
	public String getQuery() {
		return this.statement;
	}
	

}
