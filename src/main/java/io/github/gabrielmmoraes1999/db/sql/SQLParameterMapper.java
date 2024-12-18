package io.github.gabrielmmoraes1999.db.sql;

import java.util.ArrayList;
import java.util.List;

public class SQLParameterMapper {

    private final String parsedQuery;
    private final List<String> parameters;

    public SQLParameterMapper(String query) {
        this.parameters = new ArrayList<>();
        this.parsedQuery = parseQuery(query);
    }

    private String parseQuery(String query) {
        StringBuilder parsedQuery = new StringBuilder();
        StringBuilder paramName = new StringBuilder();
        boolean inParam = false;

        for (char c : query.toCharArray()) {
            if (c == ':') {
                inParam = true;
                paramName = new StringBuilder();
            } else if (inParam && (Character.isLetterOrDigit(c) || c == '_')) {
                paramName.append(c);
            } else {
                if (inParam) {
                    inParam = false;
                    parameters.add(paramName.toString());
                    parsedQuery.append('?');
                }
                parsedQuery.append(c);
            }
        }

        // Handle the last parameter if it ends the query
        if (inParam) {
            parameters.add(paramName.toString());
            parsedQuery.append('?');
        }

        return parsedQuery.toString();
    }

    public String getParsedQuery() {
        return parsedQuery;
    }

    public List<String> getParameters() {
        return parameters;
    }
}
