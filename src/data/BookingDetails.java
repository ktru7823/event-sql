package usyd.it.olympics.data;

import java.util.Date;
import java.util.HashMap;

public class BookingDetails extends GeneralTupleConverter {
	private static String[] attribs =  { "journey_id", "vehicle_code", 	"origin_name", 		"dest_name", 	"when_departs", 	"when_arrives", 	"booker_name", "bookedfor_name", "when_booked"};
	private static Class<?>[]  classes = { Integer.class, String.class, 	String.class, 	String.class, 	Date.class, Date.class, String.class, String.class, Date.class};
	
	public BookingDetails() {
		super(attribs, classes);
	}

	public static String getSummary(HashMap<String, Object> details) {
    	String summary = "Vehicle " + details.get("vehicle_code");
    	summary = summary.concat("\nbooked for " + details.get("bookedfor_name") + " by " + details.get("bookedby_name") + " on " + (Date) details.get("when_booked"));
    	summary = summary.concat("\nTravelling from " + details.get("origin_name") + " to " + details.get("dest_name"));
    	summary = summary.concat("\nLeaving " + (Date) details.get("when_departs") + " and arrived " + (Date) details.get("when_arrives"));
		return summary;
	}



}
