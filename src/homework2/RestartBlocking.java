package homework2;


import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class RestartBlocking {
    private static final String DB_URL = "jdbc:postgresql://localhost:5432/postgres";
    private static final String USER = "postgres";
    private static final String PASS = "postgres";
    private static final int NUM_TASKS = 30;
    private static final int THREADS = 10;

    public static void main(String[] args) throws Exception {
        ExecutorService executor = Executors.newFixedThreadPool(THREADS);
        for (int i = 0; i < NUM_TASKS; i++) {
            final int taskId = i;
            executor.submit(() -> runTaskWithRestart(taskId));
        }
        executor.shutdown();
        executor.awaitTermination(5, TimeUnit.MINUTES);
        System.out.println("[DONE] All tasks completed successfully via client restart.");
    }

    private static void runTaskWithRestart(int taskId) {
        boolean committed = false;
        int attempts = 0;
        while (!committed) {
            attempts++;
            try (Connection conn = DriverManager.getConnection(DB_URL, USER, PASS)) {
                conn.setAutoCommit(false);
                try (PreparedStatement stmt = conn.prepareStatement(
                        "UPDATE blocking_data SET data_value = data_value + 1 WHERE data_id = ?")) {
                    int[] targets = getTaskTargetRows(taskId);
                    for (int targetId : targets) {
                        stmt.setInt(1, targetId);
                        stmt.executeUpdate();
                    }
                    conn.commit();
                    committed = true;
                    System.out.printf("Task %d committed successfully on attempt %d%n", taskId, attempts);
                } catch (SQLException e) {
                    conn.rollback();
                    if ("40P01".equals(e.getSQLState())) {
                        Thread.sleep(10 + new Random().nextInt(20));
                    } else {
                        throw e;
                    }
                }
            } catch (Exception e) {
                // Retry loop continues until commit
            }
        }
    }

    private static int[] getTaskTargetRows(int taskId) {
        Random r = new Random(taskId);
        return new int[]{r.nextInt(100) + 1, r.nextInt(100) + 1, r.nextInt(100) + 1};
    }
}