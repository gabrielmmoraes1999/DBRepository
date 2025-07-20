package io.github.gabrielmmoraes1999.db;

import com.zaxxer.hikari.HikariDataSource;
import javax.sql.DataSource;

public class ConnectionPoolManager {

    private static HikariDataSource hikariDataSource;

    public static void setHikariDataSource(HikariDataSource hikariDataSource) {
        ConnectionPoolManager.hikariDataSource = hikariDataSource;

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("Encerrando pool de conexões...");
            ConnectionPoolManager.close();
        }));
    }

    public static HikariDataSource getHikariDataSource() {
        return hikariDataSource;
    }

    public static boolean isPresent() {
        return hikariDataSource != null;
    }

    public static DataSource getDataSource() {
        return hikariDataSource.getDataSource();
    }

    public static void close() {
        if (hikariDataSource != null)
            hikariDataSource.close();
    }

}
