package io.github.gabrielmmoraes1999.db.util;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class FindBy {

    private String methodName;
    private final Object[] args;
    private final Class<?> entityClass;

    private final List<String> conditions;
    private final List<Object> params;

    public FindBy(String methodName, Object[] args, Class<?> entityClass) {
        this.methodName = methodName;
        this.args = args;
        this.entityClass = entityClass;
        this.conditions = new ArrayList<>();
        this.params = new ArrayList<>();
        this.process();
    }

    private void process() {
        String findBy = null;

        if (methodName.contains("OrderBy")) {
            String[] part = methodName.split("OrderBy");
            this.methodName = part[0];
        }

        if (methodName.contains("FindBy")) {
            methodName = methodName.substring(methodName.indexOf("FindBy"));
        }

        if (methodName.startsWith("findBy") || methodName.startsWith("FindBy")) {
            findBy = methodName.substring(6);
        }

        if (Objects.isNull(findBy)) {
            throw new IllegalArgumentException("Unable to get findBy.");
        }

        String lastCondition = "AND";
        String[] tokens = findBy.split("(?=And|Or)");

        for (int i = 0; i < tokens.length; i++) {
            String token = tokens[i];

            if (token.startsWith("And")) {
                lastCondition = "AND";
                token = token.substring(3);
            } else if (token.startsWith("Or") && !token.startsWith("OrderBy")) {
                lastCondition = "OR";
                token = token.substring(2);
            }

            String operator = "="; // Padrão é Equals
            if (token.startsWith("GreaterThan")) {
                operator = ">";
                token = token.substring(11);
            } else if (token.startsWith("LessThan")) {
                operator = "<";
                token = token.substring(8);
            } else if (token.startsWith("NotEquals")) {
                operator = "!=";
                token = token.substring(9);
            } else if (token.startsWith("Like")) {
                operator = "LIKE";
                token = token.substring(4);
            }

            String columnName = Function.getColumnName(entityClass, token);

            conditions.add(String.format("%s %s ?", columnName, operator));
            params.add(args[i]);

            if (i > 0) {
                conditions.set(i, String.join(" ", lastCondition, conditions.get(i)));
            }
        }
    }

    public String getWhere() {
        return String.format("WHERE %S", String.join(" ", getConditions()));
    }

    private List<String> getConditions() {
        return conditions;
    }

    public List<Object> getParams() {
        return params;
    }
}
