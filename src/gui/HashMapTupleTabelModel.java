package usyd.it.olympics.gui;

import java.util.ArrayList;
import java.util.HashMap;

import javax.swing.table.AbstractTableModel;

import usyd.it.olympics.data.TupleConverter;


@SuppressWarnings("serial")
class HashMapTupleTabelModel extends AbstractTableModel {
	private ArrayList<HashMap<String, Object>> tuples;

	private final String [] columnNames;
	private final Class<?>[] columnClasses;
	private final String [] attributeNames;

	private TupleConverter shim;

	public Class<?> getColumnClass(int columnIndex) {
		return columnClasses[columnIndex];
	}

	public HashMapTupleTabelModel(TupleConverter tupleConverter, String[] columns) {
		this(tupleConverter, columns, null);
	}

	public HashMapTupleTabelModel(TupleConverter tupleConverter) {
		this(tupleConverter, null, null);
	}


	/*
	 * Construct new table model for a List of HashMaps, with mappings handled by a TupleConverter 
	 * @param tupleConverter object to handle mappings of objects stored in HashMaps
	 * @param columns which attributes to include from the tuple in the table
	 * @param headings what to call the displayed tuples
	 */
	public HashMapTupleTabelModel(TupleConverter tupleConverter, String[] columns, String[] headings) {
		shim = tupleConverter;
		if (columns==null) {
			attributeNames = shim.getAttributeNames();
			// FIXME: should check that arrays are the same lengths.
			columnNames = headings==null? shim.getAttributeNames() : headings;
			columnClasses = shim.getColumnClasses();
		} else {
			// FIXME: should check that arrays are the same lengths.
			attributeNames = columns;			
			// Work out the class associated with each attribute
			columnClasses = projectClasses(columns, shim.getAttributeNames(), shim.getColumnClasses());
			columnNames = headings==null? shim.getAttributeNames() : headings;    		
		}
		tuples = new ArrayList<HashMap<String, Object>>();
	}

	/**
	 * 
	 * @param projection projection of attributes
	 * @param attributes source attribute names
	 * @param srcClasses source attribute classes
	 * @return array of classes corresponding to the attributes in columns
	 */
	private static Class<?>[] projectClasses(String[] projection, String[] attributes, Class<?>[] attributeClasses) {
		HashMap<String,Integer> attributeIndex = new HashMap<String,Integer>();
		for(int i=0; i<attributes.length; ++i)
			attributeIndex.put(attributes[i], i);

		Class<?>[] classes = new Class<?>[projection.length];
		for(int j=0; j<projection.length; ++j)
			classes[j] = attributeClasses[attributeIndex.get(projection[j])];
		return classes;
	}

	/**
	 * Update the table with newly supplied data
	 * @param newCars New list of cars
	 */
	public void update(ArrayList<HashMap<String, Object>> newtuples) {
		tuples = newtuples;
		super.fireTableDataChanged();
	}

	public HashMap<String, Object> getTuple(int row) {
		return tuples.get(row);
	}

	/*
	 * AbstractTableModel methods
	 */
	@Override
	public String getColumnName(int col) {
		return columnNames[col];
	}

	@Override
	public int getColumnCount() {
		return columnNames.length;
	}

	@Override
	public int getRowCount() {
		return tuples.size();
	}

	@Override
	public Object getValueAt(int row, int col) {
		Object o=null;
		HashMap<String, Object> tuple = tuples.get(row);
		o = tuple!=null && col>=0 && col<columnNames.length ? tuple.get(attributeNames[col]) : null;
		return o;
	}

}