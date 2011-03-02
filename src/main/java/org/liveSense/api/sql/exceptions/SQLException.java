package org.liveSense.api.sql.exceptions;

import java.io.Serializable;

public class SQLException extends Exception implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 2373880253277343932L;
	private String msg;


	public SQLException() {
		super();
	}

	public SQLException(String msg) {
		super(msg);
		this.msg = msg;
	}

	public SQLException(Throwable cause) {
		super(cause);
		this.msg = cause.getMessage();
	}

	public SQLException(String msg, Throwable cause) {
		super(msg, cause);
		this.msg = msg;
	}

	public String getMessage() {
		return msg;
	}
}
