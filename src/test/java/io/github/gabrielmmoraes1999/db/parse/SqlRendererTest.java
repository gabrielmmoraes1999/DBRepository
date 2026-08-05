package io.github.gabrielmmoraes1999.db.parse;

import io.github.gabrielmmoraes1999.db.entity.User;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.*;

class SqlRendererTest {

    @Test
    void renderInAndNotInWithCorrectKeywordAndPlaceholders() {
        ParsedQuery inQuery = MethodNameParser.parse("findByIdIn");
        String inSql = SqlRenderer.toSql(inQuery, new Object[]{Arrays.asList(1, 2, 3)}, User.class);
        assertTrue(inSql.contains("p1.ID IN (?, ?, ?)"));
        assertFalse(inSql.contains("NOT IN"));

        ParsedQuery notInQuery = MethodNameParser.parse("findByIdNotIn");
        String notInSql = SqlRenderer.toSql(notInQuery, new Object[]{Arrays.asList(1, 2)}, User.class);
        assertTrue(notInSql.contains("p1.ID NOT IN (?, ?)"));
    }

    @Test
    void renderMultiParamWithInUsesCorrectArgIndex() {
        ParsedQuery query = MethodNameParser.parse("findByNameAndIdIn");
        String sql = SqlRenderer.toSql(query, new Object[]{"Ana", Arrays.asList(1, 2)}, User.class);

        assertTrue(sql.contains("p1.NAME = ?"));
        assertTrue(sql.contains("p1.ID IN (?, ?)"));
    }

    @Test
    void emptyInBecomesFalseCondition() {
        ParsedQuery query = MethodNameParser.parse("findByIdIn");
        String sql = SqlRenderer.toSql(query, new Object[]{Collections.emptyList()}, User.class);
        assertTrue(sql.contains("1 = 0"));
    }

    @Test
    void renderCountExistsAndDelete() {
        Object[] args = new Object[]{18};

        String countSql = SqlRenderer.toSql(MethodNameParser.parse("countByAgeGreaterThan"), args, User.class);
        assertTrue(countSql.startsWith("SELECT COUNT(*) FROM USERS p1 WHERE"));

        String existsSql = SqlRenderer.toSql(MethodNameParser.parse("existsByAgeGreaterThan"), args, User.class);
        assertTrue(existsSql.startsWith("SELECT COUNT(*) FROM USERS p1 WHERE"));

        String deleteSql = SqlRenderer.toSql(MethodNameParser.parse("deleteByAgeLessThan"), args, User.class);
        assertTrue(deleteSql.startsWith("DELETE FROM USERS WHERE"));
        assertTrue(deleteSql.contains("AGE < ?"));
        assertFalse(deleteSql.contains("p1."));
    }
}
