package io.github.gabrielmmoraes1999.db.parse;

public class OrderBy {

    public final String field;
    public final boolean desc;

    public OrderBy(String field, boolean desc) {
        this.field = field;
        this.desc = desc;
    }

}
