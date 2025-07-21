package io.github.gabrielmmoraes1999.db;

import com.zaxxer.hikari.HikariDataSource;
import javax.sql.DataSource;

public class ConnectionPoolManager {

    private static HikariDataSource dataSource;

    public static void setHikariDataSource(HikariDataSource hikariDataSource) {
        ConnectionPoolManager.dataSource = hikariDataSource;

        Runtime.getRuntime().addShutdownHook(new Thread(ConnectionPoolManager::close));
    }

    public static boolean isPresent() {
        return dataSource != null;
    }

    public static DataSource getDataSource() {
        return dataSource;
    }

    public static void close() {
        if (dataSource != null)
            dataSource.close();
    }

}
