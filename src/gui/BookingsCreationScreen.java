package usyd.it.olympics.gui;

import java.awt.Component;
import java.awt.GridLayout;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.util.Calendar;
import java.util.Date;

import javax.swing.Box;
import javax.swing.JButton;
import javax.swing.JLabel;
import javax.swing.JSpinner;
import javax.swing.JTextField;
import javax.swing.SpinnerDateModel;

import usyd.it.olympics.OlympicsDBClient;

public class BookingsCreationScreen extends GuiScreen {
    private SpinnerDateModel startDates = new SpinnerDateModel(new Date(), null, null, Calendar.HOUR_OF_DAY);
	private JTextField txtMemberName;
	private JTextField txtVehicleCode;

    public BookingsCreationScreen(OlympicsDBClient r) {
        super(r);
        panel_.setLayout(new GridLayout(0, 2, 0, 0));

        JLabel lblCarName = new JLabel("Vehicle:");
        panel_.add(lblCarName);
        txtVehicleCode = new JTextField();
        panel_.add(txtVehicleCode);
        
        JLabel lblMember = new JLabel("Member:");
        panel_.add(lblMember);
        txtMemberName = new JTextField();
        panel_.add(txtMemberName);

        JLabel lblStartDate = new JLabel("Start");
        panel_.add(lblStartDate);

        JSpinner startDateSelect = new JSpinner();
        startDateSelect.setModel(startDates);
        panel_.add(startDateSelect);

        Component horizontalGlue = Box.createHorizontalGlue();
        panel_.add(horizontalGlue);

        JButton btnSubmitBooking = new JButton("Submit Booking");
        btnSubmitBooking.addActionListener(new ActionListener() {
            public void actionPerformed(ActionEvent arg0) {
                client_.makeBooking(getMember(), getVehicle(), getDepartTime());
            }
        });
        panel_.add(btnSubmitBooking);
    }

    private String getVehicle() {
        // TODO: allow pick from spinner
        return txtVehicleCode.getText();
    }

    private String getMember() {
        return txtMemberName.getText();
    }
    
	/**
	 * @return
	 */
	private Date getDepartTime() {
		return (Date)startDates.getValue();
	}

	public void startBooking(String vehicle, Date departs) {
		if (vehicle!=null) txtVehicleCode.setText(vehicle);
		if (departs!=null) startDates.setValue(departs);
	}

}
