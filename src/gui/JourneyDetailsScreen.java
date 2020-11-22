package usyd.it.olympics.gui;

import java.awt.Component;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.util.Date;
import java.util.HashMap;

import javax.swing.BoxLayout;
import javax.swing.JButton;
import javax.swing.JTextArea;

import usyd.it.olympics.OlympicsDBClient;

/**
 * Very simple details screen, cloned from HomeScreen
 * @author Bryn
 */
public class JourneyDetailsScreen extends GuiScreen {

    private final JTextArea description;
    private final JButton btnMakeBooking;
	private HashMap<String, Object> journey;
    public JourneyDetailsScreen(OlympicsDBClient r) {
        super(r);
        panel_.setLayout(new BoxLayout(panel_, BoxLayout.Y_AXIS));

        description = new JTextArea();
        description.setEditable(false);
        panel_.add(description);
        
        // Extra panel to include buttons
        // Include a button to allow a booking to be made for the current bay
        btnMakeBooking = new JButton("Make Booking");
        btnMakeBooking.setAlignmentX(Component.CENTER_ALIGNMENT);
        panel_.add(btnMakeBooking);
        btnMakeBooking.setEnabled(false);
        btnMakeBooking.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent arg0) {
                client_.startBooking((String) journey.get("vehicle_code"), (Date) journey.get("when_departs"));
            }
        });   
    }

    public void showJourneyDetails(HashMap<String, Object> tuple) {
        journey = tuple;
        btnMakeBooking.setEnabled(true);
        
        String s = "Vehicle: " + journey.get("vehicle_code") + " has " + (Integer) journey.get("capacity") + " seats"
                + "\nLeaves " + journey.get("origin_name") + " at " + (Date) journey.get("when_departs")
                + "\nand travels to " + journey.get("dest_name") + " for " + (Date) journey.get("when_arrives")
                + "\nCurrently there are  " + (Integer) journey.get("nbooked") + " bookings for this journey";

        
        description.setText(s);
    }
}
