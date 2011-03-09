package org.liveSense.api.sql;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;

import org.apache.commons.beanutils.BeanUtilsBean;

public class AnnotationHelper {
	
    public static Annotation[] findClassAnnotation(Class<?> clazz) {
        return clazz.getAnnotations();
    }

    public static Annotation[] findMethodAnnotation(Class<?> clazz, String methodName) {

        Annotation[] annotations = null;
        try {
            Class<?>[] params = null;
            Method method = clazz.getDeclaredMethod(methodName, params);
            if (method != null) {
                annotations = method.getAnnotations();
            }
        } catch (SecurityException e) {
            e.printStackTrace();
        } catch (NoSuchMethodException e) {
            e.printStackTrace();
        }
        return annotations;
    }

    public static Annotation[] findFieldAnnotation(Class<?> clazz, String fieldName) {
        Annotation[] annotations = null;
        try {
            Field field = clazz.getDeclaredField(fieldName);
            if (field != null) {
                annotations = field.getAnnotations();
            }
        } catch (SecurityException e) {
        } catch (NoSuchFieldException e) {
        }
        return annotations;
    }

    public static List<Field> getAllFields(Class<?> type) {
    	return getAllFields(null, type);
    }

    
    private static List<Field> getAllFields(List<Field> fields, Class<?> type) {
        if (fields == null) fields = new ArrayList<Field>();
    	for (Field field: type.getDeclaredFields()) {
            fields.add(field);
        }

        if (type.getSuperclass() != null) {
            fields = getAllFields(fields, type.getSuperclass());
        }

        return fields;
    }
    
    public static Map<String, Object> getObjectAsMap(Object bean) throws IllegalAccessException, InvocationTargetException, NoSuchMethodException {
    	Map<String, Object> ret = new HashMap<String, Object>();
    	
    	List<Field> fields = getAllFields(bean.getClass());
  	
    	for (Field fld : fields) {
    		Annotation[] annotations = fld.getAnnotations();
    		for (int i=0; i<annotations.length; i++) {
    			if (annotations[i] instanceof Column) {
    				Column col = (Column)annotations[i];
    				ret.put(col.name(), BeanUtilsBean.getInstance().getPropertyUtils().getNestedProperty(bean, fld.getName()));
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
    	List<Field> fields = getAllFields(bean.getClass());
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
