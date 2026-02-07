package io.github.gabrielmmoraes1999.db.parse;

import java.util.*;

public final class SqlTemplate {

    private final String originalSql;
    private final String parsedSql;
    private final List<String> paramOrder;
    private final Map<String, Object> values = new HashMap<>();

    private SqlTemplate(String sql) {
        this.originalSql = sql;
        this.paramOrder = new ArrayList<>();
        this.parsedSql = parse(sql, paramOrder);
    }

    public static SqlTemplate of(String sql) {
        return new SqlTemplate(sql);
    }

    public SqlTemplate set(String name, Object value) {
        values.put(name.toUpperCase(), value);
        return this;
    }

    public String getSql() {
        return parsedSql;
    }

    public List<Object> getBindValues() {
        List<Object> binds = new ArrayList<>();

        for (String param : paramOrder) {
            if (!values.containsKey(param)) {
                throw new IllegalStateException("Parâmetro não informado: :" + param);
            }
            binds.add(values.get(param));
        }

        return binds;
    }

    private static String parse(String sql, List<String> order) {

        StringBuilder out = new StringBuilder();
        StringBuilder buffer = new StringBuilder();

        boolean readingParam = false;

        for (int i = 0; i < sql.length(); i++) {
            char c = sql.charAt(i);

            if (c == ':') {
                readingParam = true;
                buffer.setLength(0);
                out.append('?');
                continue;
            }

            if (readingParam) {
                if (Character.isLetterOrDigit(c) || c == '_') {
                    buffer.append(c);
                } else {
                    readingParam = false;
                    order.add(buffer.toString().toUpperCase());
                    out.append(c);
                }
            } else {
                out.append(c);
            }
        }

        if (readingParam) {
            order.add(buffer.toString().toUpperCase());
        }

        return out.toString();
    }

}
