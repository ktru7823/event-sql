package usyd.it.olympics.data;

import java.util.Date;
import java.util.HashMap;

public class GeneralTupleConverter implements TupleConverter {
	public final String[] attributeNames;
	public final Class<?>[] attributeClasses;
	
	public GeneralTupleConverter(String[] attributes, Class<?>[] classes) {
		attributeNames = attributes;
		attributeClasses = classes;
	}
	
	@Override
	public String[] getAttributeNames() {
		return attributeNames;
	}

	@Override
	public Class<?>[] getColumnClasses() {
		return attributeClasses;
	}
	
	public Integer getInt(String attribute, HashMap<String, Object> tuple) {
		Object value =  tuple.get(attribute);
		return  ((value!=null && value instanceof Integer) ? (Integer) value : null);
	}

	public String getString(String attribute, HashMap<String, Object> tuple) {
		Object value =  tuple.get(attribute);
		return  ((value!=null && value instanceof String) ? (String) value : null);
	}
	
	public Date getDate(Date attribute, HashMap<String, Object> tuple) {
		Object value =  tuple.get(attribute);
		return  ((value!=null && value instanceof Date) ? (Date) value : null);
	}

}
