package io.github.gabrielmmoraes1999.db.parse;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class MethodNameParserTest {

    @Test
    void parseFindByName() {
        ParsedQuery query = MethodNameParser.parse("findByName");

        assertEquals(QueryType.SELECT, query.type);
        assertEquals(1, query.orGroups.size());
        assertEquals(1, query.orGroups.get(0).size());
        assertEquals("name", query.orGroups.get(0).get(0).field);
        assertEquals(Operator.EQ, query.orGroups.get(0).get(0).operator);
    }

    @Test
    void parseCountAndExistsAndDelete() {
        assertEquals(QueryType.COUNT, MethodNameParser.parse("countByAgeGreaterThan").type);
        assertEquals(QueryType.EXISTS, MethodNameParser.parse("existsByName").type);
        assertEquals(QueryType.DELETE, MethodNameParser.parse("deleteByAgeLessThan").type);
        assertEquals(QueryType.DELETE, MethodNameParser.parse("removeById").type);
    }

    @Test
    void parseInAndNotInAndBetween() {
        ParsedQuery inQuery = MethodNameParser.parse("findByIdIn");
        assertEquals(Operator.IN, inQuery.orGroups.get(0).get(0).operator);

        ParsedQuery notInQuery = MethodNameParser.parse("findByIdNotIn");
        assertEquals(Operator.NOT_IN, notInQuery.orGroups.get(0).get(0).operator);

        ParsedQuery betweenQuery = MethodNameParser.parse("findByAgeBetween");
        assertEquals(Operator.BETWEEN, betweenQuery.orGroups.get(0).get(0).operator);
    }

    @Test
    void parseAndOrOrderBy() {
        ParsedQuery query = MethodNameParser.parse("findByNameAndAgeGreaterThanOrAgeLessThanOrderByNameDesc");

        assertEquals(2, query.orGroups.size());
        assertEquals(2, query.orGroups.get(0).size());
        assertEquals(1, query.orGroups.get(1).size());
        assertEquals(1, query.orderByList.size());
        assertEquals("name", query.orderByList.get(0).field);
        assertTrue(query.orderByList.get(0).desc);
    }

    @Test
    void rejectInvalidPrefix() {
        assertThrows(IllegalArgumentException.class, () -> MethodNameParser.parse("updateByName"));
    }
}
