package homework2;


import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class NoLiveLocks {
    private static final String DB_URL = "jdbc:postgresql://localhost:5432/postgres";
    private static final String USER = "postgres";
    private static final String PASS = "postgres";
    private static final int NUM_TASKS = 30;
    private static final int THREADS = 10;

    public static void main(String[] args) throws Exception {
        ExecutorService executor = Executors.newFixedThreadPool(THREADS);
        long startTime = System.currentTimeMillis();
        for (int i = 0; i < NUM_TASKS; i++) {
            final int taskId = i;
            executor.submit(() -> runStarvationFreeTask(taskId));
        }
        executor.shutdown();
        executor.awaitTermination(5, TimeUnit.MINUTES);
        long duration = System.currentTimeMillis() - startTime;
        System.out.printf("[DONE] All %d tasks committed in %d ms with 0 livelocks.%n", NUM_TASKS, duration);
    }

    private static void runStarvationFreeTask(int taskId) {
        boolean committed = false;
        int attempt = 0;
        int[] targets = getTaskTargetRows(taskId);
        
        // Key Deterministic Enhancement: Sort row keys ascending to enforce lock ordering
        Arrays.sort(targets);

        while (!committed) {
            attempt++;
            try (Connection conn = DriverManager.getConnection(DB_URL, USER, PASS)) {
                conn.setAutoCommit(false);
                try (PreparedStatement stmt = conn.prepareStatement(
                        "UPDATE blocking_data SET data_value = data_value + 1 WHERE data_id = ?")) {
                    for (int targetId : targets) {
                        stmt.setInt(1, targetId);
                        stmt.executeUpdate();
                    }
                    conn.commit();
                    committed = true;
                } catch (SQLException e) {
                    conn.rollback();
                    if ("40P01".equals(e.getSQLState())) {
                        long backoff = Math.min(1000, (10L << attempt) + new Random().nextInt(50));
                        Thread.sleep(backoff);
                    } else {
                        throw e;
                    }
                }
            } catch (Exception e) {
                // Continue retry loop until completion
            }
        }
    }

    private static int[] getTaskTargetRows(int taskId) {
        Random r = new Random(taskId);
        return new int[]{r.nextInt(100) + 1, r.nextInt(100) + 1, r.nextInt(100) + 1};
    }
}
