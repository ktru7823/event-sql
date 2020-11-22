package usyd.it.olympics.data;

public interface TupleConverter {

	String[] getAttributeNames();

	Class<?>[] getColumnClasses();
	
}
