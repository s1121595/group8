import java.sql.*;
import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicLong;

public class ContinuousBenchmark {
    public static int num_threads = 16;
    public static long benchmark_duration_ms = 30000;
    Connection con;
    Random rand = new Random(123456);
    int num_tuple = 50000;
    double max_value = 50.0f;
    AtomicLong total_committed_transactions;

    AtomicLong total_rollbacks;
    AtomicLong deadlock_count;
    AtomicLong duplicate_key_count;
    AtomicLong other_sql_errors;

    volatile boolean running = true;

    public ContinuousBenchmark(){
        total_committed_transactions = new AtomicLong(0);
        total_rollbacks = new AtomicLong(0);
        deadlock_count = new AtomicLong(0);
        duplicate_key_count = new AtomicLong(0);
        other_sql_errors = new AtomicLong(0);
    }

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

    public static void main(String[] args){
        ContinuousBenchmark bench = new ContinuousBenchmark();
        bench.createDB();
        ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(num_threads);
        System.out.println("[INFO] Starting continuous transaction stream for " + (benchmark_duration_ms / 1000) + " seconds...");
        double start_time = System.currentTimeMillis();
        for(int i = 0; i < num_threads; i++){
            executor.execute(bench.new WorkerThread(i));
        }
        try {
            Thread.sleep(benchmark_duration_ms);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        bench.running = false;
        executor.shutdownNow();
        double end_time = System.currentTimeMillis();
        double elapsed_sec = (end_time - start_time) / 1000.0;
        System.out.println("\n--- BENCHMARK RESULTS ---");
        System.out.println("Total Number of Threads: " + num_threads);
        System.out.println("Total Committed Transactions: " + bench.total_committed_transactions.get());
        System.out.println("Elapsed Time: " + elapsed_sec + " seconds");
        System.out.println("Throughput: " + (bench.total_committed_transactions.get() / elapsed_sec) + " transactions/sec");

        System.out.println("\n--- DIAGNOSTIC / ROLLBACK BREAKDOWN ---");
        System.out.println("Total Rollbacks: " + bench.total_rollbacks.get());
        System.out.println("Deadlocks (40P01): " + bench.deadlock_count.get());
        System.out.println("Duplicate Key Violations (23505): " + bench.duplicate_key_count.get());
        System.out.println("Other SQL Errors: " + bench.other_sql_errors.get());
    }

    public class WorkerThread implements Runnable {
        final int workerId;
        int dynamicTupleIdCounter = num_tuple;

        private Connection con;
        private PreparedStatement selectPs;
        private PreparedStatement insertPs;
        private Statement scanStmt;
        private PreparedStatement updatePs;

        public WorkerThread(int id){
            this.workerId = id;
        }

        private void initResources() throws SQLException {
            con = ConnectionFactory.getDefaultParameterConnection(ConnectionFactory.POSTGRESQL);
            con.setAutoCommit(false);
            con.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
            selectPs = con.prepareStatement("SELECT data_value FROM blocking_data WHERE data_id = ?;");
            insertPs = con.prepareStatement("INSERT INTO blocking_data VALUES (?, ?, ?);");
            scanStmt = con.createStatement();
            updatePs = con.prepareStatement("UPDATE blocking_data SET data_value = ?, modified_by = ? WHERE data_id = ?;");
        }

        private void closeResources() {
            try { if (selectPs != null) selectPs.close(); } catch (SQLException ex) { ex.printStackTrace(); }
            try { if (insertPs != null) insertPs.close(); } catch (SQLException ex) { ex.printStackTrace(); }
            try { if (scanStmt != null) scanStmt.close(); } catch (SQLException ex) { ex.printStackTrace(); }
            try { if (updatePs != null) updatePs.close(); } catch (SQLException ex) { ex.printStackTrace(); }
            try { if (con != null && !con.isClosed()) con.close(); } catch (SQLException ex) { ex.printStackTrace(); }
        }

        private boolean executeStochasticTransaction() {
            try {
                double p = rand.nextDouble();
                if (p < 0.6) {
                    double sum = 0;
                    for (int q = 0; q < 3; q++) {
                        int randomId = rand.nextInt(dynamicTupleIdCounter);
                        selectPs.setInt(1, randomId);
                        ResultSet rs = selectPs.executeQuery();
                        if (rs.next()) {
                            sum += rs.getDouble("data_value");
                        }
                        rs.close();
                    }
                } else if (p >= 0.6 && p < 0.7) {
                    insertPs.setInt(1, dynamicTupleIdCounter++);
                    insertPs.setDouble(2, rand.nextDouble() * max_value);
                    insertPs.setInt(3, workerId);
                    insertPs.executeUpdate();
                } else if (p >= 0.7 && p < 0.8) {
                    ResultSet rs = scanStmt.executeQuery("SELECT sum(data_value) FROM blocking_data;");
                    if (rs.next()) {
                        double tableSum = rs.getDouble(1);
                    }
                    rs.close();
                } else {
                    for (int q = 0; q < 3; q++) {
                        updatePs.setDouble(1, rand.nextDouble() * max_value);
                        updatePs.setInt(2, workerId);
                        updatePs.setInt(3, rand.nextInt(dynamicTupleIdCounter));
                        updatePs.executeUpdate();
                    }
                }
                con.commit();
                return true;
            } catch (SQLException e) {
                total_rollbacks.incrementAndGet();
                String sqlState = e.getSQLState();

                try {
                    if (con != null) con.rollback();
                } catch (SQLException ex) {
                    ex.printStackTrace();
                }

                if ("40P01".equals(sqlState)) {
                    deadlock_count.incrementAndGet();
                    try {
                        int backoffMillis = (int) (Math.pow(2, rand.nextInt(4)) * 5 + rand.nextInt(20));
                        Thread.sleep(backoffMillis);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                    }
                    return false;
                } else if ("23505".equals(sqlState)) {
                    duplicate_key_count.incrementAndGet();
                    return false;
                } else {
                    other_sql_errors.incrementAndGet();
                    System.err.println("[SQLState " + sqlState + "] " + e.getMessage());
                    closeResources();
                    con = null;
                    return false;
                }
            }
        }

        @Override
        public void run() {
            try {
                initResources();
                while (running) {
                    if (con == null) {
                        initResources();
                    }
                    boolean success = false;
                    while (running && !success) {
                        success = executeStochasticTransaction();
                    }
                    if (success) {
                        total_committed_transactions.incrementAndGet();
                    }
                }
            } catch (SQLException e) {
                e.printStackTrace();
            } finally {
                closeResources();
            }
        }
    }
}