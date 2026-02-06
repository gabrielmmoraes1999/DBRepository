package io.github.gabrielmmoraes1999.db.parse;

public class Condition {

    public final String field;
    public final Operator operator;

    public Condition(String field, Operator operator) {
        this.field = field;
        this.operator = operator;
    }

}
