package org.liveSense.service.DataSourceProvider;

public class NoDataSourceFound extends Exception {
	
	
	public NoDataSourceFound(Throwable e) {
		super(e);
	}
	
	public NoDataSourceFound(String message) {
		super(message);
	}
	
	public NoDataSourceFound(String message, Throwable e) {
		super(message, e);
	}

}
