package com.beenbank;

import java.sql.*;
import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;

public class JDBCUtil {
    /**
     *
     */
    private static final long serialVersionUID = 1L;

    public static void main(String[] args) {
        /*JDBCUtil jdbcWrapper = JDBCUtil.getJDBCInstance();
        Object[] obj = new Object[]{"1","1","1","1"};
        List<Object[]> list = new ArrayList<Object[]>();
        list.add(obj);
        jdbcWrapper.doBatch("INSERT INTO result VALUES(?,?,?,?)", list);*/

        Set<String > sets =  new HashSet<String>();
        sets.add("jkdfj");
        Iterator<String> it =sets.iterator();
        while (it.hasNext()){
            String a = it.next();
            String b = it.next();
        }




    }

    private static JDBCUtil jdbcInstance = null;
    private static LinkedBlockingQueue<Connection> dbConnectionPool = new LinkedBlockingQueue<Connection>();

    static {
        try {
            Class.forName("com.mysql.jdbc.Driver");
        } catch (ClassNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    public static JDBCUtil getJDBCInstance() {
        if (jdbcInstance == null) {

            synchronized (JDBCUtil.class) {
                if (jdbcInstance == null) {
                    jdbcInstance = new JDBCUtil();
                }
            }
        }

        return jdbcInstance;
    }

    private JDBCUtil() {

        for (int i = 0; i < 5; i++) {

            try {
             /* Connection conn = DriverManager.getConnection("jdbc:mysql://172.16.162.101:3306/datacenter_test", "datacenter",
                        "datacenter");*/
         Connection conn = DriverManager.getConnection("jdbc:mysql://rm-bp1t425pa1y459zyr.mysql.rds.aliyuncs.com:3306/bi_platform",
                        "dw_master",
                        "5e0bdd4b56e544c3526e2bbeb4a98bfB");

                dbConnectionPool.put(conn);
            } catch (Exception e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }

        }

    }

    public synchronized Connection getConnection() {
        while (0 == dbConnectionPool.size()) {
            try {
                Thread.sleep(20);
            } catch (InterruptedException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }

        return dbConnectionPool.poll();
    }

    public int[] doBatch(String sqlText, List<Object[]> paramsList) {
        Connection conn = getConnection();
        PreparedStatement preparedStatement = null;
        int[] result = null;
        try {
            conn.setAutoCommit(false);
            preparedStatement = conn.prepareStatement(sqlText);

            for (Object[] parameters : paramsList) {
                for (int i = 0; i < parameters.length; i++) {
                    preparedStatement.setObject(i + 1, parameters[i]);
                }

                preparedStatement.addBatch();
            }

            result = preparedStatement.executeBatch();

            conn.commit();

        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } finally {
            if (preparedStatement != null) {
                try {
                    preparedStatement.close();
                } catch (SQLException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            }

            if (conn != null) {
                try {
                    dbConnectionPool.put(conn);
                } catch (InterruptedException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            }
        }

        return result;
    }

    public void doQuery(String sqlText, Object[] paramsList, ExecuteCallBack callBack) throws Exception {

        Connection conn = getConnection();
        PreparedStatement preparedStatement = null;
        ResultSet result = null;
        try {

            preparedStatement = conn.prepareStatement(sqlText);

            if(paramsList!=null){
                for (int i = 0; i < paramsList.length; i++) {
                    preparedStatement.setObject(i + 1, paramsList[i]);
                }
            }

            result = preparedStatement.executeQuery();

            callBack.resultCallBack(result);

        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } finally {
            if (preparedStatement != null) {
                try {
                    preparedStatement.close();
                } catch (SQLException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                    throw new Exception("database exception");
                }
            }

            if (conn != null) {
                try {
                    dbConnectionPool.put(conn);
                } catch (InterruptedException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            }
        }

    }
}

interface ExecuteCallBack {
    void resultCallBack(ResultSet result) throws Exception;
}