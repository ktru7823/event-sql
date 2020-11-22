package usyd.it.olympics;
//HELLO WORLD

//Howdey Partner

/**
 * Database back-end class for simple gui.
 *
 * The DatabaseBackend class defined in this file holds all the methods to
 * communicate with the database and pass the results back to the GUI.
 *
 *
 * Make sure you update the dbname variable to your own database name. You
 * can run this class on its own for testing without requiring the GUI.
 */
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Properties;
import java.util.Calendar;
import java.text.SimpleDateFormat;
import java.sql.Timestamp;
import java.sql.Time;

/**
 * Database interfacing backend for client. This class uses JDBC to connect to
 * the database, and provides methods to obtain query data.
 *
 * Most methods return database information in the form of HashMaps (sets of
 * key-value pairs), or ArrayLists of HashMaps for multiple results.
 *
 * @author Bryn Jeffries {@literal <bryn.jeffries@sydney.edu.au>}
 */
public class DatabaseBackend {

	///////////////////////////////
	/// DB Connection details
	/// These are set in the constructor so you should never need to read or
	/// write to them yourself
	///////////////////////////////
	private final String dbUser;
	private final String dbPass;
	private final String connstring;

	///////////////////////////////
	/// Student Defined Functions
	///////////////////////////////

	// COMPLETE (Kenny): checkLogin, getMemberDetails, getEventResults,
	// getEventsOfSport,
	// findJourneys, getJourneyDetails, getSports

	// INCOMPLETE: HomeScreen.showMemberDetails,
	// getMemberBookings, makeBooking, getBookingDetails

	// FIXME: limit rows retrieved from each query?
	// edit gui

	///// Login and Member //////

	/**
	 * Validate memberID details
	 *
	 * Implements Core Functionality (a)
	 *
	 * @return basic details of user if username is for a valid memberID and
	 *         password is correct
	 * @throws OlympicsDBException
	 * @throws SQLException
	 */
	public HashMap<String, Object> checkLogin(String member, char[] password) throws OlympicsDBException {
		// Note that password is a char array for security reasons.
		// Don't worry about this: just turn it into a string to use in this
		// function
		// with "new String(password)"

		String pword = new String(password);
		int valid = 0;

		HashMap<String, Object> details = null;
		Connection conn = null;
		try {
			conn = getConnection();

			// Query whether login (memberID, password) is correct...
			PreparedStatement stmt = null;
			String query = "SELECT COUNT(member_id) AS count " + "FROM member "
					+ "WHERE member_id = ? AND pass_word = ?;";

			stmt = conn.prepareStatement(query);
			stmt.setString(1, member);
			stmt.setString(2, pword);
			ResultSet results = stmt.executeQuery();
			while (results.next()) {
				valid = results.getInt("count");
			}

			if (stmt != null) {
				stmt.close();
			}

			if (valid >= 1) {
				// Populate with basic record data
				details = getMemberDetails(member);
			}
		} catch (Exception e) {
			throw new OlympicsDBException("Error checking login details", e);
		} finally {
			reallyClose(conn);
		}

		return details;
	}

	/**
	 * Obtain details for the current memberID
	 *
	 * @param memberID
	 *
	 * @return Details of member
	 * @throws OlympicsDBException
	 */
	public HashMap<String, Object> getMemberDetails(String memberID) throws OlympicsDBException {
		HashMap<String, Object> details = new HashMap<String, Object>();

		details.put("member_id", memberID);

		Connection conn = null;
		try {
			conn = getConnection();

			String memberType = findMemberType(memberID, conn);
			if (memberType == null) {
				reallyClose(conn);
				return null;
			}
			details.put("member_type", memberType);

			PreparedStatement stmt = null;

			String query = "SELECT title, given_names, family_name, country_code, country_name, place_name "
					+ "FROM (member NATURAL JOIN country) JOIN place ON (accommodation = place_id) "
					+ "WHERE member_id = ?;";

			stmt = conn.prepareStatement(query);
			stmt.setString(1, memberID);
			ResultSet results = stmt.executeQuery();

			while (results.next()) {
				details.put("title", results.getString("title"));
				details.put("first_name", results.getString("given_names"));
				details.put("family_name", results.getString("family_name"));

				String country = results.getString("country_name");
				if (country == null) {
					country = results.getString("country_code");
				}
				details.put("country_name", country);
				details.put("residence", results.getString("place_name"));
			}

			if (stmt != null) {
				stmt.close();
			}

			// FIXME: the other member types
			// Some attributes fetched may depend upon member_type
			// This is for an athlete
			switch (memberType) {
			case "athlete": // get medals
				int medals[] = getAthleteMedals(memberID, conn);
				details.put("num_gold", Integer.valueOf(medals[0]));
				details.put("num_silver", Integer.valueOf(medals[1]));
				details.put("num_bronze", Integer.valueOf(medals[2]));
				break;
			case "staff": // TODO

				break;
			case "official": // get roles
				ArrayList<String> roles = getRoles(memberID, conn);
				details.put("roles", roles);
				break;
			default:
				// something
			}

			int bookingCount = getBookingCount(memberID, conn);
			details.put("num_bookings", Integer.valueOf(bookingCount));

		} catch (Exception e) {
			throw new OlympicsDBException("Error fetching member details", e);
		} finally {
			reallyClose(conn);
		}

		return details;
	}

	private String findMemberType(String memberID, Connection conn) throws Exception {
		int athlete = 0;
		int official = 0;
		int staff = 0;
		PreparedStatement stmt = null;

		String query = "SELECT COUNT(A.member_id) as athlete, COUNT(O.member_id) as official, COUNT(S.member_id) as staff "
				+ "FROM member M LEFT OUTER JOIN athlete A ON (M.member_id = A.member_id) LEFT OUTER JOIN official O ON (M.member_id = O.member_id) LEFT OUTER JOIN staff S ON (M.member_id = S.member_id) "
				+ "WHERE M.member_id = ?;";

		stmt = conn.prepareStatement(query);
		stmt.setString(1, memberID);
		ResultSet results = stmt.executeQuery();

		while (results.next()) {
			athlete = results.getInt("athlete");
			staff = results.getInt("staff");
			official = results.getInt("official");
		}

		if (stmt != null) {
			stmt.close();
		}

		if (athlete == 1) {
			return "athlete";
		} else if (official == 1) {
			return "official";
		} else if (staff == 1) {
			return "staff";
		}
		return null;
	}

	private int[] getAthleteMedals(String memberID, Connection conn) throws Exception {

		int medals[] = new int[3];

		PreparedStatement stmt = null;

		String query = "SELECT COUNT(*) " + "FROM participates " + "WHERE medal = 'G' AND athlete_id = ?;";

		stmt = conn.prepareStatement(query);
		stmt.setString(1, memberID);
		ResultSet results = stmt.executeQuery();

		String teamQuery = "SELECT COUNT(*)"
				+ "FROM teammember TM LEFT OUTER JOIN team T ON (TM.team_name = T.team_name)"
				+ "WHERE TM.athlete_id = ? AND TM.event_id = T.event_id AND medal = 'G';";
		stmt = conn.prepareStatement(teamQuery);
		stmt.setString(1, memberID);
		ResultSet teamResults = stmt.executeQuery();

		if (results.next() && teamResults.next()) {
			medals[0] = results.getInt("count") + teamResults.getInt("count");
		}

		query = "SELECT COUNT(*) " + "FROM participates " + "WHERE medal = 'S' AND athlete_id = ?;";
		stmt = conn.prepareStatement(query);
		stmt.setString(1, memberID);
		results = stmt.executeQuery();

		teamQuery = "SELECT COUNT(*)" + "FROM teammember TM LEFT OUTER JOIN team T ON (TM.team_name = T.team_name)"
				+ "WHERE TM.athlete_id = ? AND TM.event_id = T.event_id AND medal = 'S';";
		stmt = conn.prepareStatement(teamQuery);
		stmt.setString(1, memberID);
		teamResults = stmt.executeQuery();

		if (results.next() && teamResults.next()) {
			medals[1] = results.getInt("count") + teamResults.getInt("count");
		}

		query = "SELECT COUNT(*) " + "FROM participates " + "WHERE medal = 'B' AND athlete_id = ?;";
		stmt = conn.prepareStatement(query);
		stmt.setString(1, memberID);
		results = stmt.executeQuery();

		teamQuery = "SELECT COUNT(*)" + "FROM teammember TM LEFT OUTER JOIN team T ON (TM.team_name = T.team_name)"
				+ "WHERE TM.athlete_id = ? AND TM.event_id = T.event_id AND medal = 'B';";
		stmt = conn.prepareStatement(teamQuery);
		stmt.setString(1, memberID);
		teamResults = stmt.executeQuery();

		if (results.next() && teamResults.next()) {
			medals[2] = results.getInt("count") + teamResults.getInt("count");
		}

		if (stmt != null) {
			stmt.close();
		}

		return medals;
	}

	private ArrayList<String> getRoles(String memberID, Connection conn) throws Exception {

		ArrayList<String> roleList = new ArrayList<String>();

		PreparedStatement stmt = null;

		String query = "SELECT role " + "FROM runsevent " + "WHERE member_id = ?;";
		stmt = conn.prepareStatement(query);
		stmt.setString(1, memberID);
		ResultSet results = stmt.executeQuery();
		while (results.next()) {
			roleList.add(results.getString("role"));
		}

		return roleList;
	}

	private int getBookingCount(String memberID, Connection conn) throws Exception {

		int bookingCount = 0;

		PreparedStatement stmt = null;

		String query = "SELECT COUNT(*) " + "FROM booking " + "WHERE booked_for = ?;";

		stmt = conn.prepareStatement(query);
		stmt.setString(1, memberID);
		ResultSet results = stmt.executeQuery();
		while (results.next()) {
			bookingCount = results.getInt("count");
		}

		return bookingCount;
	}

	////////// Events //////////

	/**
	 * Get all of the events listed in the olympics for a given sport
	 *
	 * @param sportId
	 *            the ID of the sport we are filtering by
	 * @return List of the events for that sport
	 * @throws OlympicsDBException
	 */
	ArrayList<HashMap<String, Object>> getEventsOfSport(Integer sportId) throws OlympicsDBException {

		ArrayList<HashMap<String, Object>> eventsList = new ArrayList<>();

		Connection conn = null;
		try {
			conn = getConnection();

			PreparedStatement stmt = null;

			String query = "SELECT * " + "FROM event JOIN place ON (sport_venue = place_id) " + "WHERE sport_id = ? "
					+ "ORDER BY event_name, event_gender;";

			stmt = conn.prepareStatement(query);
			stmt.setInt(1, sportId);
			ResultSet results = stmt.executeQuery();

			while (results.next()) {
				HashMap<String, Object> event = new HashMap<String, Object>();
				event.put("event_id", Integer.valueOf(results.getInt("event_id")));
				event.put("sport_id", sportId);
				event.put("event_name", results.getString("event_name"));
				event.put("event_gender", results.getString("event_gender"));
				event.put("event_start", results.getTimestamp("event_start"));

				event.put("sport_venue", results.getString("place_name"));

				eventsList.add(event);
			}
		} catch (Exception e) {
			throw new OlympicsDBException("Error fetching event details", e);
		} finally {
			reallyClose(conn);
		}

		return eventsList;
	}

	/**
	 * Retrieve the results for a single event
	 *
	 * @param eventId
	 *            the key of the event
	 * @return a hashmap for each result in the event.
	 * @throws OlympicsDBException
	 */
	ArrayList<HashMap<String, Object>> getResultsOfEvent(Integer eventId) throws OlympicsDBException {

		ArrayList<HashMap<String, Object>> eventResults = new ArrayList<>();
		Connection conn = null;
		try {
			conn = getConnection();
			String query = "SELECT COUNT(*) FROM participates WHERE event_id = ?;";
			PreparedStatement stmt = null;

			stmt = conn.prepareStatement(query);
			stmt.setInt(1, eventId);
			ResultSet results = stmt.executeQuery();
			if (results.next()) {
				if (results.getInt("count") >= 1) {
					eventResults = getResultsIndividual(eventId, conn);
				} else {
					eventResults = getResultsTeam(eventId, conn);
				}
			}

		} catch (Exception e) {
			throw new OlympicsDBException("Error fetching event results", e);
		} finally {
			reallyClose(conn);
		}
		return eventResults;
	}

	private ArrayList<HashMap<String, Object>> getResultsIndividual(Integer eventId, Connection conn) throws Exception {

		ArrayList<HashMap<String, Object>> eventResults = new ArrayList<>();
		PreparedStatement stmt = null;

		String query = "SELECT (family_name || ', ' || given_names) AS name, country_code, country_name, medal "
				+ "FROM (participates JOIN member ON (member_id = athlete_id)) NATURAL JOIN country "
				+ "WHERE event_id = ? " + "ORDER BY name;";

		stmt = conn.prepareStatement(query);
		stmt.setInt(1, eventId);
		ResultSet results = stmt.executeQuery();

		while (results.next()) {
			HashMap<String, Object> athlete = new HashMap<String, Object>();
			athlete.put("participant", results.getString("name"));
			String country = results.getString("country_name");
			if (country == null) {
				country = results.getString("country_code");
			}
			athlete.put("country_name", country);

			String medal = results.getString("medal");
			if (medal == null) {
				athlete.put("medal", medal);
			} else if (medal.equals("G")) {
				athlete.put("medal", "Gold");
			} else if (medal.equals("S")) {
				athlete.put("medal", "Silver");
			} else if (medal.equals("B")) {
				athlete.put("medal", "Bronze");
			}
			eventResults.add(athlete);
		}

		if (stmt != null) {
			stmt.close();
		}

		return eventResults;
	}

	private ArrayList<HashMap<String, Object>> getResultsTeam(Integer eventId, Connection conn) throws Exception {

		ArrayList<HashMap<String, Object>> eventResults = new ArrayList<>();

		PreparedStatement stmt = null;

		String query = "SELECT DISTINCT team_name, country_name, country_code, medal "
				+ "FROM team NATURAL JOIN country " + "WHERE event_id = ?" + "ORDER BY team_name;";
		stmt = conn.prepareStatement(query);
		stmt.setInt(1, eventId);
		ResultSet results = stmt.executeQuery();

		while (results.next()) {
			HashMap<String, Object> team = new HashMap<String, Object>();
			team.put("participant", results.getString("team_name"));

			String country = results.getString("country_name");
			if (country == null) {
				country = results.getString("country_code");
			}
			team.put("country_name", country);

			String medal = results.getString("medal");
			if (medal == null) {
				team.put("medal", medal);
			} else if (medal.equals("G")) {
				team.put("medal", "Gold");
			} else if (medal.equals("S")) {
				team.put("medal", "Silver");
			} else if (medal.equals("B")) {
				team.put("medal", "Bronze");
			}

			eventResults.add(team);
		}

		if (stmt != null) {
			stmt.close();
		}

		return eventResults;
	}
	/////// Journeys ////////

	/**
	 * Array list of journeys from one place to another on a given date
	 *
	 * @param journeyDate
	 *            the date of the journey
	 * @param fromPlace
	 *            the origin, starting place.
	 * @param toPlace
	 *            the destination, place to go to.
	 * @return a list of all journeys from the origin to destination
	 */
	ArrayList<HashMap<String, Object>> findJourneys(String fromPlace, String toPlace, Date journeyDate)
			throws OlympicsDBException {
		ArrayList<HashMap<String, Object>> journeyList = new ArrayList<>();

		Connection conn = null;

		try {
			conn = getConnection();

			PreparedStatement stmt = null;

			String query = "SELECT journey_id, vehicle_code, P1.place_name AS from_place_name, P2.place_name AS to_place_name, depart_time, arrive_time, (capacity - nbooked) AS available_seats "
					+ "FROM journey NATURAL JOIN vehicle JOIN place P1 ON (from_place = P1.place_id) JOIN place P2 ON (to_place = P2.place_id) "
					+ "WHERE P1.place_name = ? AND P2.place_name = ? AND Date(depart_time) = ?;";

			java.sql.Date journeyDateSQL = new java.sql.Date(journeyDate.getTime());

			stmt = conn.prepareStatement(query);
			stmt.setString(1, fromPlace);
			stmt.setString(2, toPlace);
			stmt.setDate(3, journeyDateSQL);
			ResultSet results = stmt.executeQuery();

			while (results.next()) {
				HashMap<String, Object> journey1 = new HashMap<String, Object>();
				journey1.put("journey_id", Integer.valueOf(results.getInt("journey_id")));
				journey1.put("vehicle_code", results.getString("vehicle_code"));
				journey1.put("origin_name", results.getString("from_place_name"));
				journey1.put("dest_name", results.getString("to_place_name"));
				journey1.put("when_departs", results.getTimestamp("depart_time"));
				journey1.put("when_arrives", results.getTimestamp("arrive_time"));
				journey1.put("available_seats", Integer.valueOf(results.getInt("available_seats")));
				journeyList.add(journey1);
			}
		} catch (Exception e) {
			throw new OlympicsDBException("Error fetching journey list", e);
		} finally {
			reallyClose(conn);
		}

		return journeyList;
	}

	ArrayList<HashMap<String, Object>> getMemberBookings(String memberID) throws OlympicsDBException {
		ArrayList<HashMap<String, Object>> bookings = new ArrayList<HashMap<String, Object>>();
		Connection conn = null;
		try {
			conn = getConnection();
			String query = "SELECT journey_id, vehicle_code, depart_time, arrive_time,P1.place_name AS origin, P2.place_name AS destination"
					+ " FROM (Booking B NATURAL JOIN Journey J),Place P1, Place P2 "
					+ "WHERE  P1.place_id = J.from_place AND " + "P2.place_id = J.to_place AND " + "booked_for = ?";
			PreparedStatement MemberBooking = conn.prepareStatement(query);
			MemberBooking.setString(1, memberID);
			ResultSet bookingResults = MemberBooking.executeQuery();
			while (bookingResults.next()) {
				HashMap<String, Object> bookingDetails = new HashMap<String, Object>();
				bookingDetails.put("journey_id", bookingResults.getInt("journey_id"));
				bookingDetails.put("vehicle_code", bookingResults.getString("vehicle_code"));
				bookingDetails.put("origin_name", bookingResults.getString("origin"));
				bookingDetails.put("dest_name", bookingResults.getString("destination"));
				bookingDetails.put("when_departs", bookingResults.getTimestamp("depart_time"));
				bookingDetails.put("when_arrives", bookingResults.getTimestamp("arrive_time"));
				bookings.add(bookingDetails);
			}
		} catch (Exception e) {
			throw new OlympicsDBException("Error fetching Member Bookings", e);
		} finally {
			reallyClose(conn);
		}

		return bookings;
	}

	/**
	 * Get details for a specific journey
	 *
	 * @return Various details of journey - see JourneyDetails.java
	 * @throws OlympicsDBException
	 * @param journey_id
	 */
	public HashMap<String, Object> getJourneyDetails(Integer journeyId) throws OlympicsDBException {
		HashMap<String, Object> journey = new HashMap<String, Object>();

		Connection conn = null;
		try {
			conn = getConnection();

			PreparedStatement stmt = null;

			String query = "SELECT journey_id, vehicle_code, P1.place_name AS from_place_name, P2.place_name AS to_place_name, "
					+ "depart_time, arrive_time, capacity, nbooked "
					+ "FROM journey NATURAL JOIN vehicle JOIN place P1 ON (from_place = P1.place_id) "
					+ "JOIN place P2 ON (to_place = P2.place_id) " + "WHERE journey_id = ?;";

			stmt = conn.prepareStatement(query);
			stmt.setInt(1, journeyId);
			ResultSet results = stmt.executeQuery();

			while (results.next()) {
				journey.put("journey_id", journeyId);
				journey.put("vehicle_code", results.getString("vehicle_code"));
				journey.put("origin_name", results.getString("from_place_name"));
				journey.put("dest_name", results.getString("to_place_name"));
				journey.put("when_departs", results.getTimestamp("depart_time"));
				journey.put("when_arrives", results.getTimestamp("arrive_time"));
				journey.put("capacity", Integer.valueOf(results.getInt("capacity")));
				journey.put("nbooked", Integer.valueOf(results.getInt("nbooked")));
			}
		} catch (Exception e) {
			throw new OlympicsDBException("Error fetching journey details", e);
		} finally {
			reallyClose(conn);
		}

		return journey;
	}

	public HashMap<String, Object> makeBooking(String byStaff, String forMember, String vehicle, Date departs)
			throws OlympicsDBException {
		HashMap<String, Object> booking = null;

		Connection conn = null;
		// validate: vehicle, forMember, departs
		// find a journey for this vehicle and dep time
		
		try {
			conn = getConnection();
			
			String memberType = findMemberType(byStaff, conn);
			if (memberType == null || !memberType.equals("staff")) {
				reallyClose(conn);
				return null;
			}
			
			HashMap<String, Object> staff = getMemberDetails(byStaff);
			String staffLastname = staff.get("family_name").toString();
			String staffFirstname = staff.get("first_name").toString();
			String staffName = staffLastname.concat(", ").concat(staffFirstname);
			
			HashMap<String, Object> member = getMemberDetails(forMember);
			if (member == null) {
				reallyClose(conn);
				return null;
			}
			Object memberLastname = member.get("family_name");
			String memberFirstname = member.get("first_name").toString();
			String memberName = memberLastname.toString().concat(", ").concat(memberFirstname);

			PreparedStatement stmt = null;

			String query = "SELECT journey_id, J.vehicle_code, depart_time, P2.place_name AS to_place_name, P1.place_name AS from_place_name, "
						+ "CURRENT_TIMESTAMP AS curr_time, (capacity - nbooked) AS available_seats "
					+ "FROM journey J JOIN vehicle V ON (J.vehicle_code = V.vehicle_code) "
						+ "JOIN place P1 ON (from_place = P1.place_id) JOIN place P2 ON (to_place = P2.place_id) "
					+ "WHERE P1.place_id = J.from_place AND P2.place_id = J.to_place "
						+ "AND (capacity - nbooked) > 0 AND J.vehicle_code = ? AND depart_time = ?;";

			stmt = conn.prepareStatement(query);
			stmt.setString(1, vehicle);
			
			java.sql.Date journeyDateSQL = new java.sql.Date(departs.getTime());
			stmt.setDate(2, journeyDateSQL);

			ResultSet results = stmt.executeQuery();

			if (results.next()) {
				
				booking = new HashMap<String, Object>();
				
				booking.put("vehicle", results.getString("vehicle_code"));
		    	booking.put("start_day", results.getTimestamp("depart_time"));
		    	booking.put("start_time", results.getTimestamp("depart_time"));
		    	booking.put("to", results.getString("to_place_name"));
		    	booking.put("from", results.getString("from_place_name"));
		    	booking.put("booked_by", staffName);
		    	booking.put("whenbooked", results.getTimestamp("curr_time"));
		    	
		    	booking.put("journey_id", Integer.valueOf(results.getInt("journey_id")));
		    	booking.put("bookedfor_name", memberName);

			}

		} catch (Exception e) {
			throw new OlympicsDBException("Error making booking", e);
		} finally {
			reallyClose(conn);
		}
		
		try {
			updateBooking(booking, byStaff, forMember);
		} catch (Exception e) {
			throw new OlympicsDBException("Error updating booking", e);
		}

		return booking;
	}

	private void updateBooking(HashMap<String, Object> booking, String byStaff, String forMember) throws Exception {

		if (booking == null) {
			return;
		}
		
		Connection conn = null;
		try {
			conn = getConnection();
			conn.setAutoCommit(false);

			Integer journeyId = (Integer)booking.get("journey_id");
			
			String query1 = "INSERT INTO booking VALUES (?, ?, ?, ?);";
			PreparedStatement stmt1 = conn.prepareStatement(query1);
			stmt1.setString(1, forMember);
			stmt1.setString(2, byStaff);
			stmt1.setTimestamp(3, (Timestamp)booking.get("whenbooked"));
			stmt1.setInt(4, journeyId);
			stmt1.executeUpdate();
			
			String query2 = "UPDATE journey SET nbooked = nbooked + 1 WHERE journey_id = ?;";
			PreparedStatement stmt2 = conn.prepareStatement(query2);
			stmt2.setInt(1, journeyId);
			stmt2.executeUpdate();
			
			conn.commit();
		} catch (Exception e) {
			if (conn != null) {
				try {
					conn.rollback();
				} catch (SQLException e1) {
					// TODO Auto-generated catch block
				}
			}
			throw new OlympicsDBException("Error updating booking", e);
		} finally {
			try {
				conn.setAutoCommit(true);
			} catch (SQLException e) {
				// TODO Auto-generated catch block
			} finally {
				reallyClose(conn);
			}
		}
	}

	public HashMap<String, Object> getBookingDetails(String memberID, Integer journeyId) throws OlympicsDBException {
		HashMap<String,Object> booking = null;
    	
    	Connection conn = null;
		try {
			conn = getConnection();

			PreparedStatement stmt = null;

			String query = "SELECT journey_id, vehicle_code, depart_time, P2.place_name AS dest_name, "
						+ "P1.place_name AS origin_name, M1.family_name || ', ' || M1.given_names AS bookedfor_name, "
						+ "M2.family_name || ', ' || M2.given_names as bookedby_name, when_booked, arrive_time "
					+ "FROM booking NATURAL JOIN journey NATURAL JOIN vehicle "
						+ "JOIN place P1 ON (from_place = P1.place_id) JOIN place P2 ON (to_place = P2.place_id) "
						+ "JOIN member M1 ON (booked_for = M1.member_id) JOIN member M2 ON (booked_by = M2.member_id) "
					+ "WHERE P1.place_id = journey.from_place AND P2.place_id = journey.to_place "
						+ "AND M1.member_id = ? AND journey_id = ?;";

			stmt = conn.prepareStatement(query);
			stmt.setString(1, memberID);
			stmt.setInt(2,  Integer.valueOf(journeyId));
			ResultSet results = stmt.executeQuery();

			if (results.next()) {
				booking = new HashMap<String,Object>();
				booking.put("journey_id", journeyId);
		        booking.put("vehicle", results.getString("vehicle_code"));
		    	booking.put("when_departs", results.getTimestamp("depart_time"));
		    	booking.put("dest_name", results.getString("dest_name"));
		    	booking.put("origin_name", results.getString("origin_name"));
		    	booking.put("bookedby_name", results.getString("bookedby_name"));
		    	booking.put("bookedfor_name", results.getString("bookedfor_name"));
		    	booking.put("when_booked", results.getTimestamp("when_booked"));
		    	booking.put("when_arrives", results.getTimestamp("arrive_time"));
			}
		} catch (Exception e) {
			throw new OlympicsDBException("Error fetching booking details", e);
		} finally {
			reallyClose(conn);
		}

        return booking;
	}

	public ArrayList<HashMap<String, Object>> getSports() throws OlympicsDBException {
		ArrayList<HashMap<String, Object>> sportsList = new ArrayList<HashMap<String, Object>>();

		Connection conn = null;
		try {
			conn = getConnection();

			Statement stmt = null;
			stmt = conn.createStatement();
			String query = "SELECT * FROM sport";

			ResultSet results = stmt.executeQuery(query);

			while (results.next()) {
				HashMap<String, Object> sport = new HashMap<String, Object>();
				sport.put("sport_id", Integer.valueOf(results.getInt("sport_id")));
				sport.put("sport_name", results.getString("sport_name"));
				sport.put("discipline", results.getString("discipline"));
				sportsList.add(sport);
			}

			if (stmt != null) {
				stmt.close();
			}
		} catch (Exception e) {
			throw new OlympicsDBException("Error fetching sport list", e);
		} finally {
			reallyClose(conn);
		}

		return sportsList;
	}

	/////////////////////////////////////////
	/// Functions below don't need
	/// to be touched.
	///
	/// They are for connecting and handling errors!!
	/////////////////////////////////////////

	/**
	 * Default constructor that simply loads the JDBC driver and sets to the
	 * connection details.
	 *
	 * @throws ClassNotFoundException
	 *             if the specified JDBC driver can't be found.
	 * @throws OlympicsDBException
	 *             anything else
	 */
	DatabaseBackend(InputStream config) throws ClassNotFoundException, OlympicsDBException {
		Properties props = new Properties();
		try {
			props.load(config);
		} catch (IOException e) {
			throw new OlympicsDBException("Couldn't read config data", e);
		}

		dbUser = props.getProperty("username");
		dbPass = props.getProperty("userpass");
		String port = props.getProperty("port");
		String dbname = props.getProperty("dbname");
		String server = props.getProperty("address");
		;

		// Load JDBC driver and setup connection details
		String vendor = props.getProperty("dbvendor");
		if (vendor == null) {
			throw new OlympicsDBException("No vendor config data");
		} else if ("postgresql".equals(vendor)) {
			Class.forName("org.postgresql.Driver");
			connstring = "jdbc:postgresql://" + server + ":" + port + "/" + dbname;
		} else if ("oracle".equals(vendor)) {
			Class.forName("oracle.jdbc.driver.OracleDriver");
			connstring = "jdbc:oracle:thin:@" + server + ":" + port + ":" + dbname;
		} else
			throw new OlympicsDBException("Unknown database vendor: " + vendor);

		// test the connection
		Connection conn = null;
		try {
			conn = getConnection();
		} catch (SQLException e) {
			throw new OlympicsDBException("Couldn't open connection", e);
		} finally {
			reallyClose(conn);
		}
	}

	/**
	 * Utility method to ensure a connection is closed without generating any
	 * exceptions
	 *
	 * @param conn
	 *            Database connection
	 */
	private void reallyClose(Connection conn) {
		if (conn != null)
			try {
				conn.close();
			} catch (SQLException ignored) {
			}
	}

	/**
	 * Construct object with open connection using configured login details
	 *
	 * @return database connection
	 * @throws SQLException
	 *             if a DB connection cannot be established
	 */
	private Connection getConnection() throws SQLException {
		Connection conn;
		conn = DriverManager.getConnection(connstring, dbUser, dbPass);
		return conn;
	}

}
