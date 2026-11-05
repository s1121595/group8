package homework3;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Homework 3 - Task 1: Realistic Transaction Workload Implementation
 * Group 8 - Advanced Databases 2026
 * 
 * Workload Probability Distribution (i.i.d. random variable p in [0, 1]):
 * - p < 0.6 (60%): Read-only Multi-point Query (reads 3 distinct tuples)
 * - 0.6 <= p < 0.7 (10%): Tuple Insertion (inserts a new tuple into blocking_data)
 * - 0.7 <= p < 0.8 (10%): Full Table Read-only Scan (SELECT SUM(data_value))
 * - p >= 0.8 (20%): Write Query (updates 3 distinct tuples)
 */
public class WorkloadBlocker implements Runnable {

    private final int taskId;
    private final int totalTuplesInDb;
    private final AtomicInteger insertedTupleCounter;
    private final AtomicBoolean isRunning;
    private final AtomicInteger totalCommittedTx;

    public WorkloadBlocker(int taskId, int totalTuplesInDb, AtomicInteger insertedTupleCounter,
                           AtomicBoolean isRunning, AtomicInteger totalCommittedTx) {
        this.taskId = taskId;
        this.totalTuplesInDb = totalTuplesInDb;
        this.insertedTupleCounter = insertedTupleCounter;
        this.isRunning = isRunning;
        this.totalCommittedTx = totalCommittedTx;
    }

    @Override
    public void run() {
        // Continuous Transaction Stream loop
        while (isRunning.get()) {
            boolean committed = false;

            while (!committed && isRunning.get()) {
                double p = ThreadLocalRandom.current().nextDouble();

                try (Connection conn = ConnectionFactory.getConnection()) {
                    conn.setAutoCommit(false);

                    if (p < 0.6) {
                        // Case 1 (60%): Read-only Multi-point Query
                        executeMultiPointQuery(conn);
                    } else if (p < 0.7) {
                        // Case 2 (10%): Tuple Insertion
                        executeTupleInsertion(conn);
                    } else if (p < 0.8) {
                        // Case 3 (10%): Full Table Read Scan
                        executeFullTableScan(conn);
                    } else {
                        // Case 4 (20%): Write Query (3 Updates)
                        executeWriteQuery(conn);
                    }

                    conn.commit();
                    committed = true;
                    totalCommittedTx.incrementAndGet();

                } catch (SQLException e) {
                    if ("40P01".equals(e.getSQLState())) {
                        // Trapping deadlock SQLState 40P01 for client-side retry
                        System.out.println("[Task " + taskId + "] Deadlock 40P01 trapped. Retrying transaction...");
                    } else {
                        System.err.println("[Task " + taskId + "] SQL Exception: " + e.getMessage());
                        try {
                            Thread.sleep(500); // Sleep briefly to prevent tight error loops if DB is disconnected
                        } catch (InterruptedException ignored) {}
                        break;
                    }
                }
            }
        }
    }

    /**
     * Case 1 (60%): Reads data_value of 3 distinct random tuples using PreparedStatement.
     */
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
        if (taskId == 0 && ThreadLocalRandom.current().nextDouble() < 0.02) {
            System.out.println("[Sample Read-Multi] Sum of 3 tuples = " + sum);
        }
    }

    /**
     * Case 2 (10%): Inserts a new tuple into blocking_data.
     */
    private void executeTupleInsertion(Connection conn) throws SQLException {
        int newId = insertedTupleCounter.incrementAndGet();
        int initialVal = ThreadLocalRandom.current().nextInt(1, 1000);

        String sql = "INSERT INTO blocking_data (data_id, data_value) VALUES (?, ?)";
        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setInt(1, newId);
            pstmt.setInt(2, initialVal);
            pstmt.executeUpdate();
        }
    }

    /**
     * Case 3 (10%): Full table read-only scan calculating SELECT SUM(data_value).
     */
    private void executeFullTableScan(Connection conn) throws SQLException {
        String sql = "SELECT SUM(data_value) AS total_sum FROM blocking_data";
        long totalSum = 0;

        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            if (rs.next()) {
                totalSum = rs.getLong("total_sum");
            }
        }
        if (taskId == 0 && ThreadLocalRandom.current().nextDouble() < 0.02) {
            System.out.println("[Sample Full Scan] Total Table Sum = " + totalSum);
        }
    }

    /**
     * Case 4 (20%): Write query executing 3 single-tuple UPDATE statements.
     */
    private void executeWriteQuery(Connection conn) throws SQLException {
        int[] targetRows = getRandomDistinctTupleIds(3);

        String sql = "UPDATE blocking_data SET data_value = data_value + 1 WHERE data_id = ?";
        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            for (int rowId : targetRows) {
                pstmt.setInt(1, rowId);
                pstmt.executeUpdate();
            }
        }
    }

    /**
     * Utility method to generate N distinct random tuple IDs.
     */
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

    /**
     * Auto-initializes the schema/base tuples and cleans up inserted tuples from previous runs.
     */
    public static void initializeDatabaseIfNecessary(Connection conn, int baseTupleCount) throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.executeUpdate("CREATE TABLE IF NOT EXISTS blocking_data (data_id INT PRIMARY KEY, data_value INT)");
        }
        
        int count = 0;
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM blocking_data WHERE data_id <= " + baseTupleCount)) {
            if (rs.next()) {
                count = rs.getInt(1);
            }
        }

        if (count < baseTupleCount) {
            System.out.println("[Database Setup] Populating base tuples up to " + baseTupleCount + "...");
            conn.setAutoCommit(false);
            String insertSql = "INSERT INTO blocking_data (data_id, data_value) VALUES (?, ?) ON CONFLICT (data_id) DO NOTHING";
            try (PreparedStatement pstmt = conn.prepareStatement(insertSql)) {
                for (int i = 1; i <= baseTupleCount; i++) {
                    pstmt.setInt(1, i);
                    pstmt.setInt(2, ThreadLocalRandom.current().nextInt(1, 100));
                    pstmt.addBatch();
                }
                pstmt.executeBatch();
            }
            conn.commit();
            conn.setAutoCommit(true);
            System.out.println("[Database Setup] Base population complete.");
        }

        // Cleanup inserted tuples above base tuple count
        String cleanupSql = "DELETE FROM blocking_data WHERE data_id > ?";
        try (PreparedStatement pstmt = conn.prepareStatement(cleanupSql)) {
            pstmt.setInt(1, baseTupleCount);
            int deleted = pstmt.executeUpdate();
            if (deleted > 0) {
                System.out.println("[Benchmark Cleanup] Removed " + deleted + " tuples inserted in previous runs.");
            }
        }
    }

    // MAIN METHOD: Entry point for running Homework 3 Task 1 Benchmark
    public static void main(String[] args) {
        int NUM_THREADS = 30;
        int BASE_TUPLE_COUNT = 1000;
        int BENCHMARK_DURATION_SECONDS = 10;

        AtomicInteger insertedTupleCounter = new AtomicInteger(BASE_TUPLE_COUNT);
        AtomicBoolean isRunning = new AtomicBoolean(true);
        AtomicInteger totalCommittedTx = new AtomicInteger(0);

        System.out.println("=== Starting Homework 3 Task 1 Continuous Stream Benchmark ===");

        // Step 1: Auto-initialize table and perform cleanup
        try (Connection conn = ConnectionFactory.getConnection()) {
            initializeDatabaseIfNecessary(conn, BASE_TUPLE_COUNT);
        } catch (SQLException e) {
            System.err.println("----------------------------------------------------------------------");
            System.err.println("[POSTGRESQL CONNECTION ERROR]");
            System.err.println("Details: " + e.getMessage());
            System.err.println("Please verify:");
            System.err.println("1. Is PostgreSQL running on localhost:5432?");
            System.err.println("2. Are user/password in ConnectionFactory.java correct?");
            System.err.println("----------------------------------------------------------------------");
        }

        // Step 2: Launch 30 concurrent threads
        ExecutorService executor = Executors.newFixedThreadPool(NUM_THREADS);
        long startTime = System.currentTimeMillis();

        for (int i = 0; i < NUM_THREADS; i++) {
            executor.submit(new WorkloadBlocker(i, BASE_TUPLE_COUNT, insertedTupleCounter, isRunning, totalCommittedTx));
        }

        // Step 3: Run benchmark for specified duration (10 seconds)
        try {
            Thread.sleep(BENCHMARK_DURATION_SECONDS * 1000L);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        // Step 4: Stop continuous stream and shutdown executor
        isRunning.set(false);
        executor.shutdown();

        long endTime = System.currentTimeMillis();
        double totalSeconds = (endTime - startTime) / 1000.0;
        int totalCommitted = totalCommittedTx.get();
        double tps = totalSeconds > 0 ? totalCommitted / totalSeconds : 0.0;

        System.out.println("");
        System.out.println("=== HW3 TASK 1 BENCHMARK RESULTS ===");
        System.out.println(String.format("Total Benchmark Time: %.2f seconds", totalSeconds));
        System.out.println("Total Committed Transactions: " + totalCommitted);
        System.out.println(String.format("Throughput (TPS): %.2f Transactions/Sec", tps));
    }
}
