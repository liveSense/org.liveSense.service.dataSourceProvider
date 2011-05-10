package org.liveSense.api.sql.exceptions;

public class NoDataSourceFound extends Exception {
	
	
	private static final long serialVersionUID = 5464666306275408477L;

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
