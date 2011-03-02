package org.liveSense.api.beanprocessors;


import java.beans.BeanInfo;
import java.beans.IntrospectionException;
import java.beans.Introspector;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class DbStandardBeanProcessorTest {

	private ResultSetMetaData rsMetaData;

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
	}

	@Before
	public void setUp() throws Exception {
			rsMetaData = new ResultSetMetaData() {
			
			String[] labels =  {null, "ID", "ID_CUSTOMER", "PASSWORD_ANNOTATED", null, null};
			String[] names =   {null, "ID", "ID_CUSTOMER", "PASSWORD_ANNOTATED", "FOUR_PART_COLUMN_NAME", "date_Field_Without_Annotation"}; 		
			
			public <T> T unwrap(Class<T> arg0) throws SQLException {
				return null;
			}
			
			public boolean isWrapperFor(Class<?> arg0) throws SQLException {
				return false;
			}
			
			public boolean isWritable(int column) throws SQLException {
				return false;
			}
			
			public boolean isSigned(int column) throws SQLException {
				return false;
			}
			
			public boolean isSearchable(int column) throws SQLException {
				return false;
			}
			
			public boolean isReadOnly(int column) throws SQLException {
				return false;
			}
			
			public int isNullable(int column) throws SQLException {
				return 0;
			}
			
			public boolean isDefinitelyWritable(int column) throws SQLException {
				return false;
			}
			
			public boolean isCurrency(int column) throws SQLException {
				return false;
			}
			
			public boolean isCaseSensitive(int column) throws SQLException {
				return false;
			}
			
			public boolean isAutoIncrement(int column) throws SQLException {
				return false;
			}
			
			public String getTableName(int column) throws SQLException {
				return "";
			}
			
			public String getSchemaName(int column) throws SQLException {
				return null;
			}
			
			public int getScale(int column) throws SQLException {
				return 0;
			}
			
			public int getPrecision(int column) throws SQLException {
				return 0;
			}
			
			public String getColumnTypeName(int column) throws SQLException {
				return null;
			}
			
			public int getColumnType(int column) throws SQLException {
				return 0;
			}
			
			public String getColumnName(int column) throws SQLException {
				return names[column];
			}
			
			public String getColumnLabel(int column) throws SQLException {
				return labels[column];
			}
			
			public int getColumnDisplaySize(int column) throws SQLException {
				return 0;
			}
			
			public int getColumnCount() throws SQLException {
				return names.length-1;
			}
			
			public String getColumnClassName(int column) throws SQLException {
				return null;
			}
			
			public String getCatalogName(int column) throws SQLException {
				return null;
			}
		};
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void mapColumnsToPropertiestest() throws IntrospectionException, SQLException {
		TestBean bean = new TestBean();

		DbStandardBeanProcessor prc = new DbStandardBeanProcessor();

		// Introspector caches BeanInfo classes for better performance
        BeanInfo beanInfo = null;
        beanInfo = Introspector.getBeanInfo(bean.getClass());

		int[] colIndex = prc.mapColumnsToProperties(rsMetaData, beanInfo.getPropertyDescriptors(), TestBean.class);
		Assert.assertTrue("Annotated column name ID mapped: ", colIndex[1] != -1 );
		Assert.assertTrue("Annotated column name ID_CUSTOMER mapped: ", colIndex[2] != -1 );
		Assert.assertTrue("Annotated column name PASSWORD_ANNOTATED mapped: ", colIndex[3] != -1 );
		Assert.assertTrue("Annotated column name FOUR_PART_COLUMN_NAME mapped: ", colIndex[4] != -1 );
		Assert.assertTrue("Annotated column name date_Field_Without_Annotation mapped: ", colIndex[5] != -1 );

	}
}
