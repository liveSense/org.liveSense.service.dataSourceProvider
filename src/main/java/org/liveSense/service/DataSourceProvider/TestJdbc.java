package org.liveSense.service.DataSourceProvider;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;

import org.apache.felix.scr.annotations.Activate;
import org.apache.felix.scr.annotations.Component;
import org.apache.felix.scr.annotations.Deactivate;
import org.apache.felix.scr.annotations.Reference;
import org.osgi.framework.BundleContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


@Component(name="Test JDBC Connection",metatype=false,label="JDBC Test connection")
public class TestJdbc {

	private static final Logger log = LoggerFactory.getLogger(TestJdbc.class);
	 
	@Reference
	DataSourceProvider provider;
	
	@Activate
	 protected void activate(BundleContext bundleContext, Map<?, ?> props) {
		Connection con=null;
		Statement stm=null;
		ResultSet rs=null;
		try {
			con = provider.getConnection("ingatlannetwork");
			stm = con.createStatement();
			rs = stm.executeQuery("SELECT * FROM VAROS");
			int numcols = rs.getMetaData().getColumnCount();
			while(rs.next()) {
				StringBuffer d = new StringBuffer();
				for(int i=1;i<=numcols;i++) {
					d.append("\t" + rs.getString(i));
				}
				log.info(d.toString());
	        }			
		} catch (NoDataSourceFound e) {
			log.error("",e);
		} catch (SQLException e) {
			log.error("",e);
		} finally {
			try { if (rs != null) rs.close(); } catch(Exception e) { }
			try { if (stm != null) stm.close(); } catch(Exception e) { }
			try { if (con != null) con.close(); } catch(Exception e) { }
		}
	}
	
	@Deactivate
	protected void deactivate() {
		
	}
}
