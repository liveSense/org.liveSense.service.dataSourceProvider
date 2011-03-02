package org.liveSense.api.dao;


/**
 * Abstract Data Access Object for Queryies
 * 
 * @param <T> - The Bean used DAO for
 */
public abstract class AbstractDao<T> {
	private int maxSize;
	private Class<T> clazz;
		
	public int getMaxSize() {
		return maxSize;
	}

	public void setMaxSize(int maxSize) {
		this.maxSize = maxSize;
	}

	public Class<T> getClazz() {
		return clazz;
	}

	public void setClazz(Class<T> clazz) {
		this.clazz = clazz;
	}

}
