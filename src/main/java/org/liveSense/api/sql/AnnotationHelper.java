package org.liveSense.api.sql;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.liveSense.core.BaseAnnotationHelper;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;

import org.apache.commons.beanutils.BeanUtilsBean;

public class AnnotationHelper extends BaseAnnotationHelper {
	
    public static Map<String, Object> getObjectAsMap(Object bean) throws IllegalAccessException, InvocationTargetException, NoSuchMethodException {
    	return getObjectAsMap(bean, (List<String>)null);
    }

    public static Map<String, Object> getObjectAsMap(Object bean, String[] fieldList) throws IllegalAccessException, InvocationTargetException, NoSuchMethodException {
    	List<String> list =  new ArrayList<String>(Arrays.asList(fieldList));
    	return getObjectAsMap(bean, list);
    }    
	
    public static Map<String, Object> getObjectAsMap(Object bean, List<String> fieldList) throws IllegalAccessException, InvocationTargetException, NoSuchMethodException {
    	Map<String, Object> ret = new HashMap<String, Object>();
    	
    	List<Field> fields = getAllFields(bean.getClass());
  	
    	for (Field fld : fields) {
    		if ((fieldList ==  null) || (fieldList.size() == 0) || (fieldList.indexOf(fld.getName()) != -1)) {
	    		Annotation[] annotations = fld.getAnnotations();
	    		for (int i=0; i<annotations.length; i++) {
	    			if (annotations[i] instanceof Column) {
	    				Column col = (Column)annotations[i];
	    				ret.put(col.name(), BeanUtilsBean.getInstance().getPropertyUtils().getNestedProperty(bean, fld.getName()));
	    			}
	    		}
    		}
    	}
    	return ret;
    }
    
    public static Set<String> getClassColumnNames(Class<?> clazz) throws IllegalAccessException, InvocationTargetException, NoSuchMethodException {
    	return getClassColumnNames(clazz,(List<String>)null);
    }
    
    public static Set<String> getClassColumnNames(Class<?> clazz, String[] fieldList) throws IllegalAccessException, InvocationTargetException, NoSuchMethodException {
    	List<String> list = new ArrayList<String>(Arrays.asList(fieldList));
    	return getClassColumnNames(clazz, list);
    }
    
    public static Set<String> getClassColumnNames(Class<?> clazz, List<String> fieldList) throws IllegalAccessException, InvocationTargetException, NoSuchMethodException {
    	Set<String> ret = new HashSet<String>();
    	ret.addAll(getClassColumnNames(clazz, fieldList, false));
    	return ret;
    }

    public static ArrayList<String> getClassColumnNames(Class<?> clazz, List<String> fieldList, boolean keepFieldOrder) throws IllegalAccessException, InvocationTargetException, NoSuchMethodException {
    	ArrayList<String> ret = new ArrayList<String>();
    
    	String[] retArray = null;
    	if (keepFieldOrder) {
    		retArray = new String[fieldList.size()];    		
    	}
    	List<Field> fields = getAllFields(clazz);
  	
    	for (Field fld : fields) {
    		if ((fieldList ==  null) || (fieldList.size() == 0) || (fieldList.indexOf(fld.getName()) != -1)) {
    			Annotation[] annotations = fld.getAnnotations();
    			for (int i=0; i<annotations.length; i++) {
    				if (annotations[i] instanceof Column) {
    					Column col = (Column)annotations[i];
    					if (keepFieldOrder) {
    						retArray[fieldList.indexOf(fld.getName())] = col.name();
    					}
    					else {
    						ret.add(col.name());
    					}
    				}
    			}
    		}
    	}
    	if (keepFieldOrder) {
    		ret.addAll(Arrays.asList(retArray));
    	}
    	return ret;
    }

    public static String getTableName(Object bean) {
    	return getTableName(bean.getClass());
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    public static String getTableName(Class clazz) {
		Entity entityAnnotation = (Entity) clazz.getAnnotation(Entity.class);
		Table tableAnnotation = (Table) clazz.getAnnotation(Table.class);
		
		String tableName = null;
    	if (entityAnnotation != null && entityAnnotation.name() != null && !"".equals(entityAnnotation.name())) tableName = entityAnnotation.name();
    	if (tableAnnotation != null && tableAnnotation.name() != null && !"".equals(tableAnnotation.name())) tableName = tableAnnotation.name();
   		return tableName;
    }

    public static String getIdColumnName(Object bean) {
    	return getIdColumnName(bean.getClass());
    }

    public static String getIdColumnName(Class<?> clazz) {
    	List<Field> fields = getAllFields(clazz);
    	for (Field fld : fields) {
    		Annotation[] annotations = fld.getAnnotations();
    		Column clm = null;
    		boolean idField = false;
    		for (int i=0; i<annotations.length; i++) {
     			if (annotations[i] instanceof Id) {
     				idField = true;
     			} else if (annotations[i] instanceof Column) {
    				clm = (Column)annotations[i];
    			}
    		}
    		if (idField && clm != null) return clm.name();
    	}
    	return null;
    }

}
