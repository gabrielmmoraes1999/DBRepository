package io.github.gabrielmmoraes1999.db;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

public class DataBase {

    protected static Connection conn;

    public static void createConnection(String url) throws SQLException {
        conn = getConnection(url);
    }

    public static void createConnection(String url, Properties info) throws SQLException {
        conn = getConnection(url, info);
    }

    public static void createConnection(String url, String user, String password) throws SQLException {
        conn = getConnection(url, user, password);
    }

    public static void createConnection(String url, String user, String password, Properties info) throws SQLException {
        conn = getConnection(url, user, password, info);
    }

    public static Connection getConnection() {
        return conn;
    }

    public static Connection getConnection(String url) throws SQLException {
        return DriverManager.getConnection(url);
    }

    public static Connection getConnection(String url, Properties info) throws SQLException {
        return DriverManager.getConnection(url, info);
    }

    public static Connection getConnection(String url, String user, String password) throws SQLException {
        return DriverManager.getConnection(url, user, password);
    }

    public static Connection getConnection(String url, String user, String password, Properties info) throws SQLException {
        if (user != null) {
            info.put("user", user);
        }

        if (password != null) {
            info.put("password", password);
        }

        return DriverManager.getConnection(url, info);
    }

    public static void commit() throws SQLException {
        conn.commit();
    }

    public static void commit(Connection connection) throws SQLException {
        connection.commit();
    }

    public static void setAutoCommit(boolean autoCommit) throws SQLException {
        conn.setAutoCommit(autoCommit);
    }

    public static void setAutoCommit(boolean autoCommit, Connection connection) throws SQLException {
        connection.setAutoCommit(autoCommit);
    }

    public static void disconnect() {
        try {
            if (conn == null) {
                return;
            }

            if (conn.isClosed()) {
                return;
            }

            conn.commit();
            conn.close();
        } catch (SQLException ignore) {

        }
    }

    public static void disconnect(Connection connection) {
        try {
            if (connection == null) {
                return;
            }

            if (connection.isClosed()) {
                return;
            }

            connection.commit();
            connection.close();
        } catch (SQLException ignore) {

        }
    }
}
