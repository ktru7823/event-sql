package usyd.it.olympics.gui;

import java.awt.Dimension;
import java.awt.FlowLayout;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;

import javax.swing.BoxLayout;
import javax.swing.JButton;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JSpinner;
import javax.swing.JTable;
import javax.swing.JTextField;
import javax.swing.ListSelectionModel;
import javax.swing.SpinnerDateModel;
import javax.swing.event.ListSelectionEvent;
import javax.swing.event.ListSelectionListener;

import usyd.it.olympics.OlympicsDBClient;
import usyd.it.olympics.data.GeneralTupleConverter;

public class JourneyFinderScreen extends GuiScreen {
	protected final GeneralTupleConverter conv = new GeneralTupleConverter(
			new String[] {"journey_id", "vehicle_code", "origin_name", "dest_name", "when_departs", "when_arrives", "available_seats"},
			new Class<?>[] {Integer.class, String.class, String.class, String.class, Date.class, Date.class, Integer.class});
	private final HashMapTupleTabelModel list = new HashMapTupleTabelModel(conv,
			new String[] {"origin_name", "dest_name", "when_departs", "when_arrives", "available_seats"},
			new String[] {"Origin", "Destination", "Leaving", "Arriving", "Availability"}
			);
	private final ListSelectionModel listSelection;
	private final JTextField txtFrom;
	private final JTextField txtDest;
	private final SpinnerDateModel startDates = new SpinnerDateModel(new Date(1305554400000L), null, null, Calendar.DAY_OF_WEEK_IN_MONTH);

	public JourneyFinderScreen(OlympicsDBClient r) {
		super(r);
		panel_.setLayout(new BoxLayout(panel_, BoxLayout.Y_AXIS));

		JPanel choicePanel = new JPanel();
		choicePanel.setMaximumSize(new Dimension(32767, 23));
		panel_.add(choicePanel);
		choicePanel.setLayout(new FlowLayout(FlowLayout.CENTER, 5, 5));

		JLabel lblAddress = new JLabel("From");
		choicePanel.add(lblAddress);
		txtFrom = new JTextField();
		txtFrom.setText("address");
		choicePanel.add(txtFrom);
		txtFrom.setColumns(20);

		JLabel lblDest = new JLabel("Destination");
		choicePanel.add(lblDest);
		txtDest = new JTextField();
		txtDest.setText("address");
		choicePanel.add(txtDest);
		txtDest.setColumns(20);

		JSpinner startDateSelect = new JSpinner();
		Calendar calendar = Calendar.getInstance();
		calendar.clear(Calendar.HOUR_OF_DAY);
		Date initDate = calendar.getTime();
		startDates.setValue(initDate);
		startDateSelect.setModel(startDates);
		startDateSelect.setEditor(new JSpinner.DateEditor(startDateSelect, "dd/MM/yyyy"));
		choicePanel.add(startDateSelect);

		JButton btnUpdate = new JButton("Search");
		btnUpdate.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent arg0) {
				client_.showMatchingJourneys(txtFrom.getText(),txtDest.getText(),(Date)startDates.getValue());
			}
		});
		choicePanel.add(btnUpdate);

		// Listing results
		JScrollPane listScrollPane = new JScrollPane();
		panel_.add(listScrollPane);

		JTable listTable = new JTable();
		listTable.setSelectionMode(ListSelectionModel.SINGLE_SELECTION);
		listTable.setModel(list);
		listSelection = listTable.getSelectionModel();
		listScrollPane.setViewportView(listTable);
		listTable.setDefaultRenderer(Date.class, new DateTimeRenderer());

		JPanel selectionOptionsPanel = new JPanel();
		panel_.add(selectionOptionsPanel);
		// Include a button to allow a booking to be made for the currently selected row
		final JButton btnGetDetails = new JButton("Get Details");
		selectionOptionsPanel.add(btnGetDetails);

		btnGetDetails.addActionListener(new ActionListener() {
			@Override
			public void actionPerformed(ActionEvent arg0) {
				int selectionIndex = listSelection.getMinSelectionIndex();
				if (selectionIndex >= 0) {
					client_.getJourneyDetails(conv.getInt("journey_id",list.getTuple(selectionIndex)));
				}
			}
		});

		// Only allow row-specific buttons to work if a row has been selected
		btnGetDetails.setEnabled(false);

		listSelection.addListSelectionListener(new ListSelectionListener() {
			@Override
			public void valueChanged(ListSelectionEvent e) {
				ListSelectionModel lsm = (ListSelectionModel) e.getSource();
				boolean isValid = !lsm.isSelectionEmpty();
				btnGetDetails.setEnabled(isValid);
			}
		});
	}

	public void showTuples(ArrayList<HashMap<String, Object>> newTuples) {
		list.update(newTuples);
	}

}
