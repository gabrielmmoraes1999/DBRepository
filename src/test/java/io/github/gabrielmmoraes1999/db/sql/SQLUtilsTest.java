package io.github.gabrielmmoraes1999.db.sql;

import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Arrays;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.*;

class SQLUtilsTest {

    @Test
    void collectionBindingStartsAtGivenPosition() throws Exception {
        try (Connection connection = DriverManager.getConnection("jdbc:h2:mem:sqlutils;DB_CLOSE_DELAY=-1")) {
            connection.createStatement().execute("CREATE TABLE ITEMS (A INT, B INT, C INT)");

            try (PreparedStatement ps = connection.prepareStatement("INSERT INTO ITEMS VALUES (?, ?, ?)")) {
                int next = SQLUtils.setPreparedStatement(ps, 1, 10);
                assertEquals(2, next);
                next = SQLUtils.setPreparedStatement(ps, next, Arrays.asList(20, 30));
                assertEquals(4, next);
                ps.executeUpdate();
            }

            try (ResultSet rs = connection.createStatement().executeQuery("SELECT A, B, C FROM ITEMS")) {
                assertTrue(rs.next());
                assertEquals(10, rs.getInt(1));
                assertEquals(20, rs.getInt(2));
                assertEquals(30, rs.getInt(3));
            }
        }
    }

    @Test
    void emptyCollectionDoesNotAdvancePosition() throws Exception {
        try (Connection connection = DriverManager.getConnection("jdbc:h2:mem:sqlutils_empty;DB_CLOSE_DELAY=-1")) {
            try (PreparedStatement ps = connection.prepareStatement("SELECT 1 WHERE 1 = 0")) {
                int next = SQLUtils.setPreparedStatement(ps, 5, Collections.emptyList());
                assertEquals(5, next);
            }
        }
    }
}
