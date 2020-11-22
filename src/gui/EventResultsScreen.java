package usyd.it.olympics.gui;

import java.util.ArrayList;
import java.util.HashMap;

import javax.swing.BoxLayout;
import javax.swing.JScrollPane;
import javax.swing.JTable;

import usyd.it.olympics.OlympicsDBClient;
import usyd.it.olympics.data.GeneralTupleConverter;
import usyd.it.olympics.data.TupleConverter;

/**
 * Simple display of results in table
 * @author bjef8061
 *
 */
public class EventResultsScreen extends GuiScreen {
	protected final TupleConverter conv = new GeneralTupleConverter(
			new String[] { "participant", "country_name", "medal"},
			new Class[] { String.class, String.class, 	String.class});
    private final HashMapTupleTabelModel tableModel = new HashMapTupleTabelModel(
    		conv, null,
    		new String[] { "Name", "Country", "Medal"});

    public EventResultsScreen(OlympicsDBClient r) {
        super(r);
        panel_.setLayout(new BoxLayout(panel_, BoxLayout.Y_AXIS));
        JTable bookingsTable = new JTable(tableModel);
        bookingsTable.setFillsViewportHeight(true);
        JScrollPane scrollPane = new JScrollPane();
        scrollPane.setViewportView(bookingsTable);
        panel_.add(scrollPane);
    }

    public void setTuples(ArrayList<HashMap<String, Object>> tuples) {
        tableModel.update(tuples==null? new ArrayList<HashMap<String, Object>>() : tuples);
    }
}
