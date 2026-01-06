package databases;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Scanner;

/**
 * Template Java code for the Mini Project assignment. ONLY MODIFY THE CODE WITHIN THE TODO/end TODO
 * BLOCKS! The automated marking system relies on the structure of the code remaining the same.
 */
public class MiniProject {

  /**
   * Execute the first query.
   * 
   * @param connection a database connection
   * @return the results of the query
   * @throws SQLException if a problem occurs when executing the query
   */
  public static Map<Integer, Integer> firstQuery(Connection connection) throws SQLException {

    System.out.println("################## 1st Query ###############");

    Map<Integer, Integer> results = new LinkedHashMap<>();

    // TODO - add code to perform the query and return the results
    // - remember to close the statement and result set

    String query = "SELECT id, miles FROM routes ORDER BY miles DESC LIMIT 5";

    try (Statement statement = connection.createStatement();
        ResultSet result = statement.executeQuery(query)) {

      while (result.next()) {
        int id = result.getInt("id");
        int miles = result.getInt("miles");
        results.put(id, miles);
      }

    }

    // end TODO

    return results;

  }

  /**
   * Execute the second query.
   * 
   * @param connection a database connection
   * @return the results of the query
   * @throws SQLException if a problem occurs when executing the query
   */
  public static Map<String, Integer> secondQuery(Connection connection) throws SQLException {

    System.out.println("################## 2nd Query ###############");

    Map<String, Integer> results = new LinkedHashMap<>();

    // TODO - add code to perform the query and return the results
    // - remember to close the statement and result set

    String query = "SELECT operator, COUNT(*) AS route_count " + "FROM routes "
        + "GROUP BY operator " + "ORDER BY route_count DESC " + "LIMIT 3";

    try (Statement statement = connection.createStatement();
        ResultSet result = statement.executeQuery(query)) {

      while (result.next()) {
        String operatorName = result.getString("operator");
        int count = result.getInt("route_count");
        results.put(operatorName, count);
      }
    }
    // end TODO

    return results;
  }

  /**
   * Execute the third query.
   * 
   * @param connection a database connection
   * @return the results of the query
   * @throws SQLException if a problem occurs when executing the query
   */
  public static Map<String, Integer> thirdQuery(Connection connection) throws SQLException {

    System.out.println("################## 3rd Query ###############");

    Map<String, Integer> results = new LinkedHashMap<>();

    // TODO - add code to perform the query and return the results
    // - remember to close the statement and result set

    String query = "SELECT routes.operator, COUNT(*) AS delay_count " + "FROM delays "
        + "JOIN routes ON delays.route = routes.id " + "GROUP BY routes.operator "
        + "ORDER BY delay_count DESC ";

    try (Statement statement = connection.createStatement();
        ResultSet result = statement.executeQuery(query)) {

      if (result.next()) {
        String operatorName = result.getString("operator");
        int delayCount = result.getInt("delay_count");
        results.put(operatorName, delayCount);
      }
    }
    // end TODO

    return results;
  }


  /**
   * Execute the fourth query.
   * 
   * @param connection a database connection
   * @return the results of the query
   * @throws SQLException if a problem occurs when executing the query
   */
  public static Map<Integer, String> fourthQuery(Connection connection) throws SQLException {

    System.out.println("################## 4th Query ###############");

    Map<Integer, String> results = new LinkedHashMap<>();

    // TODO - add code to perform the query and return the results
    // - remember to close the statement and result set

    String query = "SELECT id, operator " + "FROM routes "
        + "WHERE (origin = 'PAD' OR destination = 'PAD') " + "AND miles < 100 " + "ORDER BY id ASC";

    try (Statement statement = connection.createStatement();
        ResultSet result = statement.executeQuery(query)) {

      while (result.next()) {
        int id = result.getInt("id");
        String operator = result.getString("operator");
        results.put(id, operator);
      }
    }

    // end TODO

    return results;
  }

  /**
   * Execute the fifth query.
   * 
   * @param connection a database connection
   * @return the results of the query
   * @throws SQLException if a problem occurs when executing the query
   */
  public static Map<String, Integer> fifthQuery(Connection connection) throws SQLException {

    System.out.println("################## 5th Query ###############");

    Map<String, Integer> results = new LinkedHashMap<>();

    // TODO - add code to perform the query and return the results
    // - remember to close the statement and result set

    String query = "SELECT stations.name, SUM(delays.delays) AS total_delays " + "FROM routes "
        + "JOIN delays ON routes.id = delays.route "
        + "JOIN stations ON routes.destination = stations.code "
        + "WHERE routes.operator = 'Scot Rail' " + "GROUP BY stations.name "
        + "ORDER BY total_delays DESC ";

    try (Statement statement = connection.createStatement();
        ResultSet result = statement.executeQuery(query)) {

      if (result.next()) {
        String stationName = result.getString("name");
        int totalDelays = result.getInt("total_delays");
        results.put(stationName, totalDelays);
      }
    }

    // end TODO

    return results;
  }

  /**
   * Create the stations table.
   * 
   * @param connection a database connection
   */
  public static void createStationsTable(Connection connection) throws SQLException {
    System.out.println("Creating stations table");

    // statement will get closed here as we are using try-with-resources
    try (PreparedStatement statement = connection.prepareStatement(
        "CREATE TABLE stations (code varchar(3) PRIMARY KEY, " + "name varchar(100));");) {
      statement.execute();
    }
  }

  /**
   * Create the routes table.
   * 
   * @param connection a database connection
   */
  public static void createRoutesTable(Connection connection) throws SQLException {
    System.out.println("Creating routes table");

    // statement will get closed here as we are using try-with-resources
    try (PreparedStatement statement =
        connection.prepareStatement("CREATE TABLE routes " + "(id int PRIMARY KEY, "
            + "FOREIGN KEY(origin) REFERENCES stations(code), origin varchar(3), "
            + "FOREIGN KEY(destination) REFERENCES stations(code), destination varchar(3), "
            + "operator varchar(100), " + "miles int);");) {
      statement.execute();
    }
  }

  /**
   * Create the routes table.
   * 
   * @param connection a database connection
   */
  public static void createDelaysTable(Connection connection) throws SQLException {
    System.out.println("Creating delays table");

    // statement will get closed here as we are using try-with-resources
    try (PreparedStatement statement = connection.prepareStatement("CREATE TABLE delays "
        + "(route int PRIMARY KEY REFERENCES routes(id), " + "delays int);");) {
      statement.execute();
    }
  }

  /**
   * Insert data into the stations table.
   * 
   * @param connection a database connection
   * @param file the file containing the data
   * @throws IOException if the file cannot be accessed
   * @throws SQLException if the data cannot be inserted
   */
  public static void insertIntoStationsTableFromFile(Connection connection, String file)
      throws IOException, SQLException {

    System.out.println("Inserting data into stations table");

    // stream, reader and statement will get closed here as we are using try-with-resources
    try (InputStream stations = MiniProject.class.getClassLoader().getResourceAsStream(file);
        PreparedStatement statement =
            connection.prepareStatement("INSERT INTO stations VALUES (?,?)");
        BufferedReader br =
            new BufferedReader(new InputStreamReader(stations, StandardCharsets.UTF_8));) {

      String currentLine = null;
      String[] brokenLine = null;

      while ((currentLine = br.readLine()) != null) {
        brokenLine = currentLine.split(",");

        for (int i = 0; i < brokenLine.length; i++) {
          statement.setString(i + 1, brokenLine[i]); // varchar values
        }

        statement.addBatch();
      }

      statement.executeBatch();

    } catch (SQLException e) {
      throw e;
    }

  }

  /**
   * Insert data into the delays table.
   * 
   * @param connection a database connection
   * @param file the file containing the data
   * @throws IOException if the file cannot be accessed
   * @throws SQLException if the data cannot be inserted
   */
  public static void insertIntoDelaysTableFromFile(Connection connection, String file)
      throws IOException, SQLException {

    System.out.println("Inserting data into delays table");

    // stream, reader and statement will get closed here as we are using try-with-resources
    try (InputStream airportFile = MiniProject.class.getClassLoader().getResourceAsStream(file);
        BufferedReader br =
            new BufferedReader(new InputStreamReader(airportFile, StandardCharsets.UTF_8));

        // TODO - complete the PreparedStatement with placeholder values

        PreparedStatement statement =
            connection.prepareStatement("INSERT INTO delays VALUES (?, ?)");

    // end TODO

    ) {

      String currentLine = null;
      String[] brokenLine = null;

      while ((currentLine = br.readLine()) != null) {
        brokenLine = currentLine.split(",");

        for (int i = 0; i < brokenLine.length; i++) {
          statement.setInt(i + 1, Integer.valueOf(brokenLine[i])); // int values
        }

        statement.addBatch();
      }

      statement.executeBatch();

    } catch (SQLException e) {
      throw e;
    }

  }

  /**
   * Insert data into the routes table.
   * 
   * @param connection a database connection
   * @param file the file containing the data
   * @throws IOException if the file cannot be accessed
   * @throws SQLException if the data cannot be inserted
   */
  public static void insertIntoRoutesTableFromFile(Connection connection, String file)
      throws IOException, SQLException {

    System.out.println("Inserting data into routes table");

    // stream, reader and statement will get closed here as we are using try-with-resources
    try (InputStream routes = MiniProject.class.getClassLoader().getResourceAsStream(file);
        PreparedStatement statement =
            connection.prepareStatement("INSERT INTO routes VALUES (?, ?, ?, ?, ?)");
        BufferedReader br =
            new BufferedReader(new InputStreamReader(routes, StandardCharsets.UTF_8));) {

      String currentLine = null;
      String[] brokenLine = null;

      while ((currentLine = br.readLine()) != null) {
        brokenLine = currentLine.split(",");

        // id,origin,destination,operator,miles

        for (int i = 0; i < brokenLine.length; i++) {
          if (i == 3 || i == 1 || i == 2) {
            statement.setString(i + 1, brokenLine[i]); // varchar values
          } else {
            statement.setInt(i + 1, Integer.valueOf(brokenLine[i])); // int values
          }
        }

        statement.addBatch();

      }

      statement.executeBatch();

    } catch (SQLException e) {
      throw e;
    }
  }

  /**
   * Drop the stations table and any associated views/tables.
   * 
   * @param connection a database connection
   */
  public static void dropStationsTable(Connection connection) throws SQLException {

    System.out.println("Dropping stations table");

    // statement will get closed here as we are using try-with-resources
    try (PreparedStatement st =
        connection.prepareStatement("DROP TABLE IF EXISTS stations CASCADE");) {
      st.execute();
    }
  }

  /**
   * Drop the routes table and any associated views/tables.
   * 
   * @param connection a database connection
   */
  public static void dropRoutesTable(Connection connection) throws SQLException {

    System.out.println("Dropping routes table");

    // statement will get closed here as we are using try-with-resources
    try (PreparedStatement statement =
        connection.prepareStatement("DROP TABLE IF EXISTS routes CASCADE");) {
      statement.execute();
    }
  }

  /**
   * Drop the delays table and any associated views/tables.
   * 
   * @param connection a database connection
   */
  public static void dropDelaysTable(Connection connection) throws SQLException {

    System.out.println("Dropping delays table");

    // statement will get closed here as we are using try-with-resources
    try (PreparedStatement statement =
        connection.prepareStatement("DROP TABLE IF EXISTS delays CASCADE");) {
      statement.execute();
    }
  }

  /**
   * Connect to your Postgres database on teachdb.cs.rhul.ac.uk.
   * 
   * @param user your username
   * @param password your password
   * @param databaseHost the host name of the database server
   * @param databaseName the name of the database
   * @return a new database connection
   */
  public static Connection connectToDatabase(String user, String password, String databaseHost,
      String port, String databaseName) throws SQLException {


    // TODO - add code to connect to the specified database here

    String jbdcURL = "jdbc:postgresql://" + databaseHost + ":" + port + "/" + databaseName;

    Connection connection = DriverManager.getConnection(jbdcURL, user, password);

    // end TODO

    if (connection != null) {
      System.out.println("Successfully connected to database..");
    } else {
      System.out.println("Failed to connect to database");
    }

    return connection;
  }


  /**
   * Main method.
   * 
   * @param args any command line arguments
   */
  public static void main(String[] args) {

    Connection connection = null;

    // scanner will get closed here as we are using try-with-resources
    try (Scanner scanner = new Scanner(System.in);) {
      System.out.println("Please enter your database username (postgres): ");
      String user = scanner.nextLine();
      if (user.isEmpty()) {
        user = "postgres";
      }
      System.out.println("Please enter your database password (postgres123): ");
      String password;
      if (System.console() != null) {
        password = new String(System.console().readPassword());
      } else {
        password = scanner.nextLine();
      }
      if (password.isEmpty()) {
        password = "postgres123";
      }


      System.out.println(
          "Please enter the host name of the database you want to connect to (localhost): ");
      String host = scanner.nextLine();
      if (host.isEmpty()) {
        host = "localhost";
      }


      System.out
          .println("Please enter the port number of the database you want to connect to (5432): ");
      String port = scanner.nextLine();
      if (port.isEmpty()) {
        port = "5432";
      }

      System.out
          .println("Please enter the name of the database you want to connect to (postgres): ");
      String name = scanner.nextLine();
      if (name.isEmpty()) {
        name = "postgres";
      }


      connection = MiniProject.connectToDatabase(user, password, host, port, name);

      dropStationsTable(connection);
      dropRoutesTable(connection);
      dropDelaysTable(connection);

      createStationsTable(connection);
      createRoutesTable(connection);
      createDelaysTable(connection);

      insertIntoStationsTableFromFile(connection, "stations.csv");
      insertIntoRoutesTableFromFile(connection, "routes.csv");
      insertIntoDelaysTableFromFile(connection, "delays.csv");

      System.out.println(firstQuery(connection));
      System.out.println(secondQuery(connection));
      System.out.println(thirdQuery(connection));
      System.out.println(fourthQuery(connection));
      System.out.println(fifthQuery(connection));

    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      try {
        // always ensure connections are closed,
        // note we couldn't use a try-with-resources here
        // because we needed the username / password entered
        if (connection != null) {
          connection.close();
        }
      } catch (SQLException e) {
        e.printStackTrace();
      }
    }

  }

}


