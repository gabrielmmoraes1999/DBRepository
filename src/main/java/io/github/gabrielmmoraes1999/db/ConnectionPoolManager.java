package io.github.gabrielmmoraes1999.db;

import com.zaxxer.hikari.HikariDataSource;
import java.sql.Connection;
import java.sql.SQLException;

public class ConnectionPoolManager {

    private static HikariDataSource hikariDataSource;

    public static void setHikariDataSource(HikariDataSource hikariDataSource) {
        ConnectionPoolManager.hikariDataSource = hikariDataSource;
        Runtime.getRuntime().addShutdownHook(new Thread(ConnectionPoolManager::close));
    }

    public static boolean isPresent() {
        return hikariDataSource != null;
    }

    public static Connection getConnection() throws SQLException {
        return hikariDataSource.getConnection();
    }

    public static void closeConnection(Connection conn) throws SQLException {
        if (ConnectionPoolManager.isPresent()) {
            conn.close();
        }
    }

    public static void close() {
        if (hikariDataSource != null)
            hikariDataSource.close();
    }

}
