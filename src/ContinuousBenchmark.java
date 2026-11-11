import java.sql.*;
import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class ContinuousBenchmark {
    // Defines the number of concurrent worker threads running the continuous stream.
    public static int num_threads = 16;


    // Defines the benchmark duration in milliseconds (e.g., 30 seconds for the live demo).
    public static long benchmark_duration_ms = 30000;

    // Declares the database connection reference for setup utilities.
    Connection con;

    // Initializes a pseudo-random number generator for probabilistic workload dicing.
    Random rand = new Random(123456);

    // Specifies the initial number of rows populated inside the blocking table.
    int num_tuple = 50000;

    // Defines the maximum upper boundary for generated data values.
    double max_value = 50.0f;

    // Thread-safe counter tracking total committed transactions across the stream.
    AtomicLong total_committed_transactions;

    // Atomic flag to signal worker threads when the benchmark duration has elapsed.
    volatile boolean running = true;

    // Constructs the ContinuousBenchmark manager and initializes counters.
    public ContinuousBenchmark(){
        total_committed_transactions = new AtomicLong(0);
    }

    // Executes arbitrary SQL DDL statements using a provided connection.
    void executeSQL(String sql, Connection con){
        java.sql.Statement stmt;
        try {
            stmt = con.createStatement();
            stmt.execute(sql);
            System.out.println("Executing "+sql);
            stmt.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    // Sets up the initial database table and batch populates initial tuples.
    void createDB(){
        try {
            Connection con = ConnectionFactory.getDefaultParameterConnection(ConnectionFactory.POSTGRESQL);
            executeSQL("DROP TABLE IF EXISTS blocking_data;", con);
            executeSQL("CREATE TABLE blocking_data ( data_id integer primary key, data_value float, modified_by integer);", con);

            PreparedStatement ps;
            con.setAutoCommit(false);
            ps = con.prepareStatement("INSERT INTO blocking_data VALUES (?, ?, ?)", ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE);

            for(int i = 0; i < num_tuple; i++){
                ps.setInt(1, i);
                ps.setDouble(2, rand.nextDouble() * max_value);
                ps.setInt(3, 0);
                ps.addBatch();
                ps.clearParameters();
            }

            int[] results = ps.executeBatch();
            con.commit();
            con.close();
            System.out.println(Arrays.toString(results));
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    // Main execution entry point for starting the continuous workload benchmark.
    public static void main(String[] args){
        ContinuousBenchmark bench = new ContinuousBenchmark();
        bench.createDB();

        // Initializes a fixed thread pool to simulate concurrent worker streams.
        ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(num_threads);

        System.out.println("[INFO] Starting continuous transaction stream for " + (benchmark_duration_ms / 1000) + " seconds...");
        double start_time = System.currentTimeMillis();

        // Submits worker task loops to the thread pool executor.
        for(int i = 0; i < num_threads; i++){
            executor.execute(bench.new WorkerThread(i));
        }

        // Keeps the main thread alive while workers process transactions continuously.
        try {
            Thread.sleep(benchmark_duration_ms);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        // Signals worker threads to stop and shuts down the executor pool.
        bench.running = false;
        executor.shutdownNow();

        double end_time = System.currentTimeMillis();
        double elapsed_sec = (end_time - start_time) / 1000.0;

        System.out.println("\n--- BENCHMARK RESULTS ---");
        System.out.println("Number of Threads:  "+ num_threads);
        System.out.println("Total Committed Transactions: " + bench.total_committed_transactions.get());
        System.out.println("Elapsed Time: " + elapsed_sec + " seconds");
        System.out.println("Throughput: " + (bench.total_committed_transactions.get() / elapsed_sec) + " transactions/sec[cite: 1, 2]");
    }

    // Represents a continuous worker thread executing stochastic transactions in a loop.
    public class WorkerThread implements Runnable {
        final int workerId;
        // Tracks newly inserted custom tuple IDs to allow safe deletion/updates during the stream.
        int dynamicTupleIdCounter = num_tuple;

        public WorkerThread(int id){
            this.workerId = id;
        }

        // Executes a single stochastic transaction attempt with retry and backoff logic.
        private boolean executeStochasticTransaction() {
            Connection con = null;
            try {
                con = ConnectionFactory.getDefaultParameterConnection(ConnectionFactory.POSTGRESQL);
                con.setAutoCommit(false);
                con.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);

                // Dice a probability value p in [0, 1] independently for the transaction.
                double p = rand.nextDouble();

                if (p < 0.6) {
                    // Case B: Read-only multi-point query reading 3 random distinct tuples[cite: 1].
                    PreparedStatement ps = con.prepareStatement("SELECT data_value FROM blocking_data WHERE data_id = ?;");
                    double sum = 0;
                    for (int q = 0; q < 3; q++) {
                        int randomId = rand.nextInt(dynamicTupleIdCounter);
                        ps.setInt(1, randomId);
                        ResultSet rs = ps.executeQuery();
                        if (rs.next()) {
                            sum += rs.getDouble("data_value");
                        }
                        rs.close();
                    }
                    ps.close();
                    // Uncomment below for console output if needed:
                    // System.out.println("Worker " + workerId + " Multi-point Sum: " + sum);

                } else if (p >= 0.6 && p < 0.7) {
                    // Case C: Insert a new tuple into the database[cite: 1].
                    PreparedStatement ps = con.prepareStatement("INSERT INTO blocking_data VALUES (?, ?, ?);");
                    ps.setInt(1, dynamicTupleIdCounter++);
                    ps.setDouble(2, rand.nextDouble() * max_value);
                    ps.setInt(3, workerId);
                    ps.executeUpdate();
                    ps.close();

                } else if (p >= 0.7 && p < 0.8) {
                    // Case D: Full table read-only scan calculating aggregate sum[cite: 1].
                    Statement stmt = con.createStatement();
                    ResultSet rs = stmt.executeQuery("SELECT sum(data_value) FROM blocking_data;");
                    if (rs.next()) {
                        double tableSum = rs.getDouble(1);
                        // System.out.println("Worker " + workerId + " Table Scan Sum: " + tableSum);
                    }
                    rs.close();
                    stmt.close();

                } else {
                    // Case E: Write query executing 3 update statements, each modifying a single tuple[cite: 1].
                    PreparedStatement ps = con.prepareStatement("UPDATE blocking_data SET data_value = ?, modified_by = ? WHERE data_id = ?;");
                    for (int q = 0; q < 3; q++) {
                        ps.setDouble(1, rand.nextDouble() * max_value);
                        ps.setInt(2, workerId);
                        ps.setInt(3, rand.nextInt(dynamicTupleIdCounter));
                        ps.executeUpdate();
                    }
                    ps.close();
                }

                // Commit transaction successfully.
                con.commit();
                return true;

            } catch (SQLException e) {
                try {
                    if (con != null) con.rollback();
                } catch (SQLException ex) {
                    ex.printStackTrace();
                }

                // Handle deadlock retries with exponential backoff and jitter (Task 4 logic).
                if ("40P01".equals(e.getSQLState())) {
                    try {
                        int backoffMillis = (int) (Math.pow(2, rand.nextInt(4)) * 5 + rand.nextInt(20));
                        Thread.sleep(backoffMillis);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                    }
                    return false;
                } else {
                    // Ignore minor operational constraints (like duplicate insert keys) and proceed.
                    return true;
                }
            } finally {
                try {
                    if (con != null && !con.isClosed()) {
                        con.close();
                    }
                } catch (SQLException ex) {
                    ex.printStackTrace();
                }
            }
        }

        @Override
        public void run() {
            // Continuous stream loop: immediately restart finished transactions while benchmark is active[cite: 1, 2].
            while (running) {
                boolean success = false;
                while (running && !success) {
                    success = executeStochasticTransaction();
                }
                if (success) {
                    total_committed_transactions.incrementAndGet();
                }
            }
        }
    }
}