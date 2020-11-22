package usyd.it.olympics.gui;

import java.awt.BorderLayout;
import java.util.ArrayList;
import java.util.HashMap;

import javax.swing.JTextArea;
import usyd.it.olympics.OlympicsDBClient;

/**
 * Very simple home screen: Nothing fancy, just prints player details in a text
 * area.
 * 
 * @author Bryn
 */
public class HomeScreen extends GuiScreen {

	private final JTextArea description;

	public HomeScreen(OlympicsDBClient r) {
		super(r);
		panel_.setLayout(new BorderLayout(0, 0));

		description = new JTextArea();
		description.setEditable(false);
		panel_.add(description);
	}

	public void showMemberDetails(HashMap<String, Object> details) {

		String message = "Hello " + details.get("title") + " " + details.get("first_name") + " "
				+ details.get("family_name");
		message = message.concat("\nYou are an: " + details.get("member_type"));
		message = message.concat("\nYou are from: " + details.get("country_name"));
		message = message.concat("\nYour live at: " + details.get("residence"));

		// This should be type-specific

		String memberType = details.get("member_type").toString();

		switch (memberType) {
		case "athlete": // get medals
			message = message.concat("\nYour medal tally is:");
			message = message.concat("\n\tGold: " + details.get("num_gold"));
			message = message.concat("\n\tSilver: " + details.get("num_silver"));
			message = message.concat("\n\tBronze: " + details.get("num_bronze"));
			break;
		case "staff": // FIXME: TODO: this
			message = message.concat("\nDo some booking, staff!");
			break;
		case "official": // get roles
			ArrayList<String> roles = (ArrayList<String>)details.get("roles");
			message = message.concat("\nYour roles are: ");
			for (int i = 0; i < roles.size(); i++) {
				message = message.concat(roles.get(i));
				if (i != roles.size() - 1) {
					message = message.concat(", ");
				}
			}
			break;
		default:

		}
		message = message.concat("\nYou have made " + details.get("num_bookings") + " bookings");

		description.setText(message);
	}
}
