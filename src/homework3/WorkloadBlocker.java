package homework3;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Homework 3 - Task 1: Realistic Transaction Workload Implementation
 * Group 8 - Advanced Databases 2026
 */
public class WorkloadBlocker implements Runnable {

    private final int taskId;
    private final int totalTuplesInDb;
    private final AtomicInteger insertedTupleCounter;

    public WorkloadBlocker(int taskId, int totalTuplesInDb, AtomicInteger insertedTupleCounter) {
        this.taskId = taskId;
        this.totalTuplesInDb = totalTuplesInDb;
        this.insertedTupleCounter = insertedTupleCounter;
    }

    private static Connection openConnection() throws SQLException {
        String url = System.getProperty("jdbc.url", System.getenv("JDBC_URL"));
        if (url == null || url.isEmpty()) {
            throw new SQLException("Set JDBC_URL or the jdbc.url system property to the database URL.");
        }

        String user = System.getProperty("jdbc.user", System.getenv("JDBC_USER"));
        String password = System.getProperty("jdbc.password", System.getenv("JDBC_PASSWORD"));
        if (user == null) {
            return DriverManager.getConnection(url);
        }
        return DriverManager.getConnection(url, user, password == null ? "" : password);
    }

    @Override
    public void run() {
        boolean committed = false;

        while (!committed) {
            double p = ThreadLocalRandom.current().nextDouble();

            try (Connection conn = openConnection()) {
                conn.setAutoCommit(false);

                if (p < 0.6) {
                    // Case 1 (60% Probability): Read-only Multi-point Query
                    executeMultiPointQuery(conn);
                } else if (p < 0.7) {
                    // Case 2 (10% Probability): Tuple Insertion
                    executeTupleInsertion(conn);
                } else if (p < 0.8) {
                    // Case 3 (10% Probability): Full Table Read-only Scan
                    executeFullTableScan(conn);
                } else {
                    // Case 4 (20% Probability): Write Query (3 Updates)
                    executeWriteQuery(conn);
                }

                conn.commit();
                committed = true;

            } catch (SQLException e) {
                if ("40P01".equals(e.getSQLState())) {
                    System.out.println("[Task " + taskId + "] Deadlock 40P01 detected. Retrying transaction...");
                } else {
                    System.err.println("[Task " + taskId + "] Fatal SQL Error: " + e.getMessage());
                    e.printStackTrace();
                    break;
                }
            }
        }
    }

    private void executeMultiPointQuery(Connection conn) throws SQLException {
        int[] targetRows = getRandomDistinctTupleIds(3);
        long sum = 0;

        String sql = "SELECT data_value FROM blocking_data WHERE data_id = ?";
        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            for (int rowId : targetRows) {
                pstmt.setInt(1, rowId);
                try (ResultSet rs = pstmt.executeQuery()) {
                    if (rs.next()) {
                        sum += rs.getLong("data_value");
                    }
                }
            }
        }
        System.out.println("[Task " + taskId + " | Read Multi-Point] Sum of tuples (" 
                + targetRows[0] + ", " + targetRows[1] + ", " + targetRows[2] + ") = " + sum);
    }

    private void executeTupleInsertion(Connection conn) throws SQLException {
        int newId = insertedTupleCounter.incrementAndGet();
        int initialVal = ThreadLocalRandom.current().nextInt(1, 1000);

        String sql = "INSERT INTO blocking_data (data_id, data_value) VALUES (?, ?)";
        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setInt(1, newId);
            pstmt.setInt(2, initialVal);
            pstmt.executeUpdate();
        }
        System.out.println("[Task " + taskId + " | Tuple Insert] Inserted Tuple ID: " 
                + newId + " with value: " + initialVal);
    }

    private void executeFullTableScan(Connection conn) throws SQLException {
        String sql = "SELECT SUM(data_value) AS total_sum FROM blocking_data";
        long totalSum = 0;

        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            if (rs.next()) {
                totalSum = rs.getLong("total_sum");
            }
        }
        System.out.println("[Task " + taskId + " | Full Table Scan] Total Table Sum = " + totalSum);
    }

    private void executeWriteQuery(Connection conn) throws SQLException {
        int[] targetRows = getRandomDistinctTupleIds(3);

        String sql = "UPDATE blocking_data SET data_value = data_value + 1 WHERE data_id = ?";
        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            for (int rowId : targetRows) {
                pstmt.setInt(1, rowId);
                pstmt.executeUpdate();
            }
        }
        System.out.println("[Task " + taskId + " | Write Query] Updated Tuples: " 
                + targetRows[0] + ", " + targetRows[1] + ", " + targetRows[2]);
    }

    private int[] getRandomDistinctTupleIds(int count) {
        Set<Integer> set = new HashSet<>();
        while (set.size() < count) {
            int randomId = ThreadLocalRandom.current().nextInt(1, totalTuplesInDb + 1);
            set.add(randomId);
        }
        int[] result = new int[count];
        int idx = 0;
        for (int val : set) {
            result[idx++] = val;
        }
        return result;
    }

    public static void cleanupInsertedTuples(Connection conn, int baseTupleCount) throws SQLException {
        String sql = "DELETE FROM blocking_data WHERE data_id > ?";
        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setInt(1, baseTupleCount);
            int deleted = pstmt.executeUpdate();
            System.out.println("[Benchmark Cleanup] Deleted " + deleted + " inserted tuples above base count " + baseTupleCount);
        }
    }

    // MAIN METHOD: Program Entry Point
    public static void main(String[] args) {
        int NUM_THREADS = 30;
        int BASE_TUPLE_COUNT = 1000;
        AtomicInteger insertedTupleCounter = new AtomicInteger(BASE_TUPLE_COUNT);

        System.out.println("=== Starting Homework 3 Task 1 Benchmark ===");

        // Step 1: Cleanup previous runs
        try (Connection conn = openConnection()) {
            cleanupInsertedTuples(conn, BASE_TUPLE_COUNT);
        } catch (SQLException e) {
            System.err.println("Cleanup note: " + e.getMessage());
        }

        // Step 2: Launch 30 concurrent threads
        ExecutorService executor = Executors.newFixedThreadPool(NUM_THREADS);
        long startTime = System.currentTimeMillis();

        for (int i = 0; i < NUM_THREADS; i++) {
            executor.submit(new WorkloadBlocker(i, BASE_TUPLE_COUNT, insertedTupleCounter));
        }

        executor.shutdown();
        try {
            executor.awaitTermination(5, TimeUnit.MINUTES);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        long endTime = System.currentTimeMillis();
        System.out.println("=== Benchmark Completed in " + (endTime - startTime) + " ms ===");
    }
}
