package org.liveSense.api.sql;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.liveSense.core.BaseAnnotationHelper;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;

import org.apache.commons.beanutils.BeanUtilsBean;

public class AnnotationHelper extends BaseAnnotationHelper {
	
    public static Map<String, Object> getObjectAsMap(Object bean) throws IllegalAccessException, InvocationTargetException, NoSuchMethodException {
    	return getObjectAsMap(bean, (List<String>)null);
    }

    public static Map<String, Object> getObjectAsMap(Object bean, String[] fieldList) throws IllegalAccessException, InvocationTargetException, NoSuchMethodException {
    	List<String> list =  new ArrayList<String>();
    	for (String field : fieldList) {
			list.add(field);
		}
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
    	Set<String> ret = new HashSet<String>();
    	
    	List<Field> fields = getAllFields(clazz);
  	
    	for (Field fld : fields) {
    		Annotation[] annotations = fld.getAnnotations();
    		for (int i=0; i<annotations.length; i++) {
    			if (annotations[i] instanceof Column) {
    				Column col = (Column)annotations[i];
    				ret.add(col.name());
    			}
    		}
    	}
    	return ret;
    }

    public static String getTableName(Object bean) {
    	return getTableName(bean.getClass());
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    public static String getTableName(Class clazz) {
		Entity annotation = (Entity) clazz.getAnnotation(Entity.class);
    	if (annotation != null) 
    		return annotation.name();
    	else
    		return null;
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
