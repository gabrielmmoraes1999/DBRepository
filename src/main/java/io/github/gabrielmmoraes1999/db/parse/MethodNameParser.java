package io.github.gabrielmmoraes1999.db.parse;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class MethodNameParser {

    private static final List<String> SUFFIXES = Arrays.asList(
            "GreaterThanEqual", "GreaterThan",
            "LessThanEqual", "LessThan",
            "NotIn", "In",
            "Between",
            "IsNotNull", "IsNull",
            "NotLike", "Like",
            "StartingWith", "EndingWith", "Containing",
            "True", "False"
    );

    public static ParsedQuery parse(String methodName) {

        if (!methodName.startsWith("findBy")) {
            throw new IllegalArgumentException("Somente findBy é suportado");
        }

        String body = methodName.substring(6);

        String[] splitOrder = body.split("OrderBy");
        String wherePart = splitOrder[0];
        String orderPart = splitOrder.length > 1 ? splitOrder[1] : null;

        ParsedQuery query = new ParsedQuery();

        parseWhere(wherePart, query);

        if (orderPart != null) {
            parseOrder(orderPart, query);
        }

        return query;
    }

    private static void parseWhere(String text, ParsedQuery query) {

        String[] orParts = text.split("Or");

        for (String or : orParts) {
            String[] andParts = or.split("And");

            List<Condition> andGroup = new ArrayList<Condition>();

            for (String part : andParts) {
                andGroup.add(parseCondition(part));
            }

            query.orGroups.add(andGroup);
        }
    }

    private static Condition parseCondition(String part) {

        for (String suffix : SUFFIXES) {
            if (part.endsWith(suffix)) {
                String field = part.substring(0, part.length() - suffix.length());
                return new Condition(
                        toSnake(field),
                        mapOperator(suffix)
                );
            }
        }

        return new Condition(toSnake(part), Operator.EQ);
    }

    private static Operator mapOperator(String suffix) {

        if ("GreaterThan".equals(suffix)) return Operator.GT;
        if ("GreaterThanEqual".equals(suffix)) return Operator.GTE;
        if ("LessThan".equals(suffix)) return Operator.LT;
        if ("LessThanEqual".equals(suffix)) return Operator.LTE;
        if ("In".equals(suffix)) return Operator.IN;
        if ("NotIn".equals(suffix)) return Operator.NOT_IN;
        if ("Between".equals(suffix)) return Operator.BETWEEN;
        if ("IsNull".equals(suffix)) return Operator.IS_NULL;
        if ("IsNotNull".equals(suffix)) return Operator.IS_NOT_NULL;
        if ("Like".equals(suffix)) return Operator.LIKE;
        if ("NotLike".equals(suffix)) return Operator.NOT_LIKE;
        if ("StartingWith".equals(suffix)) return Operator.STARTS_WITH;
        if ("EndingWith".equals(suffix)) return Operator.ENDS_WITH;
        if ("Containing".equals(suffix)) return Operator.CONTAINS;
        if ("True".equals(suffix)) return Operator.TRUE;
        if ("False".equals(suffix)) return Operator.FALSE;

        return Operator.EQ;
    }

    private static void parseOrder(String text, ParsedQuery query) {

        int i = 0;

        while (i < text.length()) {

            int asc = text.indexOf("Asc", i);
            int desc = text.indexOf("Desc", i);

            boolean isAsc = false;
            boolean isDesc = false;
            int end;

            if (asc != -1 && (desc == -1 || asc < desc)) {
                isAsc = true;
                end = asc;
            } else if (desc != -1) {
                isDesc = true;
                end = desc;
            } else {
                // último campo sem Asc/Desc
                String field = text.substring(i);
                query.orderByList.add(new OrderBy(toSnake(field), false));
                break;
            }

            String field = text.substring(i, end);
            query.orderByList.add(new OrderBy(toSnake(field), isDesc));

            i = end + (isAsc ? 3 : 4);
        }
    }

    static String toSnake(String s) {
        return s.replaceAll("([a-z])([A-Z])", "$1_$2").toLowerCase();
    }

}
