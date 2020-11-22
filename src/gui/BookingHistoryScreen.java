package usyd.it.olympics.gui;
import java.awt.Component;
/**
 * Bookings Screen: Display Booking information
 * Author Bryn Jeffries
 */
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;

import javax.swing.BoxLayout;
import javax.swing.JButton;
import javax.swing.JScrollPane;
import javax.swing.JTable;
import javax.swing.ListSelectionModel;
import javax.swing.event.ListSelectionEvent;
import javax.swing.event.ListSelectionListener;

import usyd.it.olympics.OlympicsDBClient;
import usyd.it.olympics.data.BookingDetails;
import usyd.it.olympics.data.GeneralTupleConverter;

public class BookingHistoryScreen extends GuiScreen {
	protected final GeneralTupleConverter bookConv = new BookingDetails();
    private final HashMapTupleTabelModel bookingList = new HashMapTupleTabelModel(
    		bookConv, 
    		new String[] { "vehicle_code","origin_name", "dest_name", "when_departs","when_arrives"},
    		new String[] { "Vehicle", "Origin","Destination","Departs","Arrives"});
    private final JButton btnGetDetails;
    private final ListSelectionModel selectionModel;
	

    public BookingHistoryScreen(OlympicsDBClient r) {
        super(r);
        panel_.setLayout(new BoxLayout(panel_, BoxLayout.Y_AXIS));
        JTable bookingsTable = new JTable(bookingList);
        bookingsTable.setDefaultRenderer(Date.class, new DateTimeRenderer());
        bookingsTable.setFillsViewportHeight(true);
        JScrollPane scrollPane = new JScrollPane();
        scrollPane.setViewportView(bookingsTable);
        panel_.add(scrollPane);
        
        bookingsTable.setSelectionMode(ListSelectionModel.SINGLE_SELECTION);
        selectionModel = bookingsTable.getSelectionModel();
        
        // Include a button to allow a booking to be made for the current bay
        btnGetDetails = new JButton("Get Details");
        btnGetDetails.setAlignmentX(Component.CENTER_ALIGNMENT);
        panel_.add(btnGetDetails);
        btnGetDetails.setEnabled(false);
        btnGetDetails.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent arg0) {
                int index = selectionModel.getMinSelectionIndex();
                if (index>=0)
						client_.showBookingDetails(bookConv.getInt("journey_id", bookingList.getTuple(index)));
                }
        }); 
        
        selectionModel.addListSelectionListener(new ListSelectionListener() {
            @Override
            public void valueChanged(ListSelectionEvent e) { 
                ListSelectionModel lsm = (ListSelectionModel)e.getSource();
                btnGetDetails.setEnabled(!lsm.isSelectionEmpty());
        }});

    }

    public void showBookings(ArrayList<HashMap<String, Object>> bookings) {
            if(bookings == null) { // Prevent nullpointer exceptions
                    bookingList.update(new ArrayList<HashMap<String, Object>>());
                    btnGetDetails.setEnabled(false);
            } else {
                    bookingList.update(bookings);
            }
    }
	
}
