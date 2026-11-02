import java.sql.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicInteger;

public class RestartBlocking {
    public static int num_taks = 30;
    public static int num_queries_per_task = 3;

    Connection con;
    Random rand = new Random(123456);
    int num_tuple = 50000;
    //    int num_tuple = 100;
    double max_value = 50.0f;
    AtomicInteger tasks_finished;

    public RestartBlocking(){
        tasks_finished = new AtomicInteger(0);
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
        RestartBlocking b = new RestartBlocking();
        b.createDB();

        ArrayList<RestartBlocking.Blocker> my_tasks = new ArrayList<RestartBlocking.Blocker>(num_taks);
        for(int task = 0; task < num_taks; task++){
            my_tasks.add(b.new Blocker(task));
        }

        ThreadPoolExecutor executor =
                (ThreadPoolExecutor) Executors.newFixedThreadPool(4);
        double start, stop;
        start = System.currentTimeMillis();
        for(Blocker task : my_tasks) {
            executor.execute(task);
        }
        executor.shutdown();

        while(!executor.isTerminated()) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        stop = System.currentTimeMillis();
        System.out.println("[DONE] Task execution after roughly " + (stop - start) + " ms finished: " + b.tasks_finished + " of " + num_taks);
    }

    public class Blocker implements Runnable  {
        final int task;
        final double [] start_range;
        final double [] stop_range;
        int deadlockRetries = 0;

        public Blocker(int id){
            this.task = id;
            this.start_range = new double[num_queries_per_task];
            this.stop_range  = new double[num_queries_per_task];
            for(int q = 0; q < num_queries_per_task; q++) {
                stop_range[q]  = rand.nextDouble() * max_value;
                start_range[q] = rand.nextDouble() * stop_range[q];
            }
        }


        private boolean attemptTransaction() {

            Connection con = null;

            PreparedStatement ps;

            try {
                con = ConnectionFactory.getDefaultParameterConnection(ConnectionFactory.POSTGRESQL);
                con.setAutoCommit(false);
                con.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
                int q = 0;
                ps = con.prepareStatement("UPDATE blocking_data SET data_value = ?, modified_by = " + task + " WHERE data_value >= ? AND data_value <= ?;", ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE);
                while(q < num_queries_per_task) {
                    ps.setDouble(1, (float) (rand.nextDouble() * max_value));
                    ps.setDouble(2, start_range[q]);
                    ps.setDouble(3, stop_range[q]);
                    ps.execute();
                    q++;
                }
                con.commit();
                return true;

            } catch (SQLException e) {
                try {
                    if (con != null) con.rollback();
                } catch (SQLException ex) {
                    ex.printStackTrace();
                }

                if ("40P01".equals(e.getSQLState())) {
                    deadlockRetries++;
                    return false;
                } else {
                    System.err.println("Task " + task + " unhandled error: " + e.getMessage());
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
            while (!attemptTransaction()) {
            }

            int num_finished = tasks_finished.incrementAndGet();
            System.out.println("Done task " + task + ". Finisher number " + num_finished + " (Required " + deadlockRetries + " deadlock retries).");
        }
    }
}