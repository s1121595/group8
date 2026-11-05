package homework3;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * Database Connection Factory for Homework 3
 */
public class ConnectionFactory {
    
    // Default PostgreSQL database credentials
    // IMPORTANT: If your local PostgreSQL password or database name is different,
    // update PASSWORD or URL here!
    private static final String URL = "jdbc:postgresql://localhost:5432/postgres";
    private static final String USER = "postgres";
    private static final String PASSWORD = "postgres";

    static {
        try {
            Class.forName("org.postgresql.Driver");
        } catch (ClassNotFoundException e) {
            System.err.println("PostgreSQL JDBC Driver not found! Ensure postgresql-42.7.14.jar is in classpath.");
        }
    }

    public static Connection getConnection() throws SQLException {
        return DriverManager.getConnection(URL, USER, PASSWORD);
    }
}
