package io.github.gabrielmmoraes1999.db.parse;

public enum Operator {
    EQ, NE,
    GT, GTE,
    LT, LTE,
    LIKE, NOT_LIKE,
    IN, NOT_IN,
    BETWEEN,
    IS_NULL, IS_NOT_NULL,
    TRUE, FALSE,
    STARTS_WITH,
    ENDS_WITH,
    CONTAINS
}
