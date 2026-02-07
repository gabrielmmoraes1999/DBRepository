package io.github.gabrielmmoraes1999.db.parse;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class MethodNameParser {

    private static final String QUERY_PATTERN  = "find|read|get|query|search|stream";
    private static final String COUNT_PATTERN  = "count";
    private static final String EXISTS_PATTERN = "exists";
    private static final String DELETE_PATTERN = "delete|remove";

    private static final Pattern PREFIX_PATTERN = Pattern.compile(
            "^(" + QUERY_PATTERN + "|" + COUNT_PATTERN + "|" + EXISTS_PATTERN + "|" + DELETE_PATTERN + ")(By.*)$",
            Pattern.CASE_INSENSITIVE
    );

    private static final Map<String, Operator> OPERATORS = new LinkedHashMap<>();

    static {
        OPERATORS.put("IsNotNull", Operator.IS_NOT_NULL);
        OPERATORS.put("IsNull", Operator.IS_NULL);
        OPERATORS.put("GreaterThanEqual", Operator.GTE);
        OPERATORS.put("GreaterThan", Operator.GT);
        OPERATORS.put("LessThanEqual", Operator.LTE);
        OPERATORS.put("LessThan", Operator.LT);
        OPERATORS.put("NotLike", Operator.NOT_LIKE);
        OPERATORS.put("Containing", Operator.CONTAINS);
        OPERATORS.put("StartingWith", Operator.STARTS_WITH);
        OPERATORS.put("EndingWith", Operator.ENDS_WITH);
        OPERATORS.put("Between", Operator.BETWEEN);
        OPERATORS.put("NotIn", Operator.NOT_IN);
        OPERATORS.put("In", Operator.IN);
        OPERATORS.put("Not", Operator.NE);
        OPERATORS.put("Like", Operator.LIKE);
        OPERATORS.put("True", Operator.TRUE);
        OPERATORS.put("False", Operator.FALSE);
    }

    private MethodNameParser() {}

    public static ParsedQuery parse(String methodName) {

        Matcher matcher = PREFIX_PATTERN.matcher(methodName);

        if (!matcher.find()) {
            throw new IllegalArgumentException("Prefixo inválido no método: " + methodName);
        }

        ParsedQuery query = new ParsedQuery();
        query.type = detectQueryType(methodName);

        String body = matcher.group(2).substring(2); // remove "By"

        String[] splitOrder = body.split("OrderBy");
        String wherePart = splitOrder[0];
        String orderPart = splitOrder.length > 1 ? splitOrder[1] : null;

        parseWhere(wherePart, query);

        if (orderPart != null && query.type == QueryType.SELECT) {
            parseOrder(orderPart, query);
        }

        return query;
    }

    private static QueryType detectQueryType(String methodName) {

        String lower = methodName.toLowerCase();

        if (lower.matches("^(" + DELETE_PATTERN + ").*")) return QueryType.DELETE;
        if (lower.matches("^(" + COUNT_PATTERN + ").*"))  return QueryType.COUNT;
        if (lower.matches("^(" + EXISTS_PATTERN + ").*")) return QueryType.EXISTS;
        if (lower.matches("^(" + QUERY_PATTERN + ").*"))  return QueryType.SELECT;

        throw new IllegalArgumentException("Tipo de query não suportado: " + methodName);
    }

    private static void parseWhere(String where, ParsedQuery query) {

        List<String> tokens = tokenize(where);

        List<Condition> currentAndGroup = new ArrayList<>();
        StringBuilder buffer = new StringBuilder();

        for (String token : tokens) {
            if ("And".equals(token)) {
                currentAndGroup.add(parseCondition(buffer.toString()));
                buffer.setLength(0);
                continue;
            }

            if ("Or".equals(token)) {
                currentAndGroup.add(parseCondition(buffer.toString()));
                query.orGroups.add(currentAndGroup);
                currentAndGroup = new ArrayList<>();
                buffer.setLength(0);
                continue;
            }

            buffer.append(token);
        }

        if (buffer.length() > 0) {
            currentAndGroup.add(parseCondition(buffer.toString()));
        }

        if (!currentAndGroup.isEmpty()) {
            query.orGroups.add(currentAndGroup);
        }
    }


    private static Condition parseCondition(String token) {
        for (Map.Entry<String, Operator> entry : OPERATORS.entrySet()) {
            String suffix = entry.getKey();

            if (token.endsWith(suffix)) {
                String field = token.substring(0, token.length() - suffix.length());
                return new Condition(toSnakeCase(field), entry.getValue());
            }
        }

        return new Condition(toSnakeCase(token), Operator.EQ);
    }

    private static void parseOrder(String order, ParsedQuery query) {
        Pattern p = Pattern.compile("([A-Z][a-zA-Z0-9]*?)(Asc|Desc)");
        Matcher m = p.matcher(order);

        while (m.find()) {
            String field = toSnakeCase(m.group(1));
            boolean desc = "Desc".equals(m.group(2));
            query.orderByList.add(new OrderBy(field, desc));
        }
    }

    private static List<String> tokenize(String text) {
        List<String> tokens = new ArrayList<>();
        Matcher m = Pattern.compile("[A-Z][a-z0-9]*").matcher(text);

        while (m.find()) {
            tokens.add(m.group());
        }
        return tokens;
    }

    private static String toSnakeCase(String s) {

        StringBuilder sb = new StringBuilder();
        char[] chars = s.toCharArray();

        for (int i = 0; i < chars.length; i++) {
            char c = chars[i];

            if (Character.isUpperCase(c) && i > 0) {
                sb.append('_');
            }
            sb.append(Character.toLowerCase(c));
        }
        return sb.toString();
    }

}
