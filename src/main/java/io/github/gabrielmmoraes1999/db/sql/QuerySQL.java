package io.github.gabrielmmoraes1999.db.sql;

import io.github.gabrielmmoraes1999.db.annotation.Delete;
import io.github.gabrielmmoraes1999.db.annotation.Param;
import io.github.gabrielmmoraes1999.db.annotation.Query;
import io.github.gabrielmmoraes1999.db.annotation.Update;

import java.lang.reflect.Method;
import java.sql.*;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class QuerySQL {

    private final Method method;
    private final Object[] args;

    public QuerySQL(Method method, Object[] args) {
        this.method = method;
        this.args = args;
    }

    @Deprecated
    public String get() {
        Query queryAnnotation = method.getAnnotation(Query.class);
        String query = queryAnnotation.value();

        if (args != null) {
            for (int index = 0; index < args.length; index++) {
                Param paramAnnotation = method.getParameters()[index].getAnnotation(Param.class);
                if (paramAnnotation != null) {
                    String paramName = paramAnnotation.value();
                    String placeholder = ":" + paramName;

                    if (query.contains(placeholder)) {
                        if (args[index] instanceof String) {
                            query = query.replace(placeholder, "'" + args[index].toString() + "'");
                        } else if (args[index] instanceof Timestamp) {
                            query = query.replace(placeholder, "'" + args[index].toString() + "'");
                        } else if (args[index] instanceof Date) {
                            query = query.replace(placeholder, "'" + args[index].toString() + "'");
                        } else if (args[index] instanceof Time) {
                            query = query.replace(placeholder, "'" + args[index].toString() + "'");
                        } else {
                            query = query.replace(placeholder, args[index].toString());
                        }
                    }
                }
            }
        }

        return query;
    }

    public PreparedStatement getPreparedStatement(Connection connection) throws SQLException {
        Query queryAnnotation = method.getAnnotation(Query.class);
        Map<String, Object> mapValues = new HashMap<>();

        if (args != null) {
            for (int index = 0; index < args.length; index++) {
                Param paramAnnotation = method.getParameters()[index].getAnnotation(Param.class);
                if (paramAnnotation != null) {
                    mapValues.put(paramAnnotation.value(), args[index]);
                }
            }
        }

        SQLParameterMapper mapper = new SQLParameterMapper(queryAnnotation.value());
        PreparedStatement preparedStatement = connection.prepareStatement(mapper.getParsedQuery());
        List<String> parameters = mapper.getParameters();

        for (int index = 0; index < parameters.size(); index++) {
            preparedStatement.setObject(index + 1, mapValues.get(parameters.get(index)));
        }

        return preparedStatement;
    }

    @Deprecated
    public String getUpdate() {
        Update updateAnnotation = method.getAnnotation(Update.class);
        String update = updateAnnotation.value();

        if (args != null) {
            for (int index = 0; index < args.length; index++) {
                Param paramAnnotation = method.getParameters()[index].getAnnotation(Param.class);
                if (paramAnnotation != null) {
                    String paramName = paramAnnotation.value();
                    String placeholder = ":" + paramName;

                    if (update.contains(placeholder)) {
                        if (args[index] instanceof String) {
                            update = update.replace(placeholder, "'" + args[index].toString() + "'");
                        } else if (args[index] instanceof Timestamp) {
                            update = update.replace(placeholder, "'" + args[index].toString() + "'");
                        } else if (args[index] instanceof Date) {
                            update = update.replace(placeholder, "'" + args[index].toString() + "'");
                        } else if (args[index] instanceof Time) {
                            update = update.replace(placeholder, "'" + args[index].toString() + "'");
                        } else if (args[index] instanceof byte[]) {
                            String hexString = bytesToHex((byte[]) args[index]);
                            update = update.replace(placeholder, "X'" + hexString + "'");
                        } else {
                            update = update.replace(placeholder, args[index].toString());
                        }
                    }
                }
            }
        }

        return update;
    }

    public int update(Connection connection) throws SQLException {
        Update updateAnnotation = method.getAnnotation(Update.class);
        Map<String, Object> mapValues = new HashMap<>();
        String update = updateAnnotation.value();
        update = update.replace(" = ", "=");
        update = update.replace("=", " = ");

        if (args != null) {
            for (int index = 0; index < args.length; index++) {
                Param paramAnnotation = method.getParameters()[index].getAnnotation(Param.class);
                if (paramAnnotation != null) {
                    String paramName = paramAnnotation.value();
                    String placeholder = ":" + paramName;

                    if (update.contains(placeholder)) {
                        update = update.replace(placeholder, "?");
                        mapValues.put(paramName, args[index]);
                    }
                }
            }
        }

        update = String.format("%s;", update);
        String[] parts = update.split(" = \\?");

        int result;
        try (PreparedStatement preparedStatement = connection.prepareStatement(update)) {
            for (int index = 0; index < parts.length - 1; index++) {
                String part = parts[index];
                String[] words = part.split(" ");
                String columnName = words[words.length - 1];
                preparedStatement.setObject(index + 1, mapValues.get(columnName));
            }

            result = preparedStatement.executeUpdate();
        }

        return result;
    }

    @Deprecated
    public String getDelete() {
        Delete deleteAnnotation = method.getAnnotation(Delete.class);
        String delete = deleteAnnotation.value();

        if (args != null) {
            for (int index = 0; index < args.length; index++) {
                Param paramAnnotation = method.getParameters()[index].getAnnotation(Param.class);
                if (paramAnnotation != null) {
                    String paramName = paramAnnotation.value();
                    String placeholder = ":" + paramName;

                    if (delete.contains(placeholder)) {
                        if (args[index] instanceof String) {
                            delete = delete.replace(placeholder, "'" + args[index].toString() + "'");
                        } else if (args[index] instanceof Timestamp) {
                            delete = delete.replace(placeholder, "'" + args[index].toString() + "'");
                        } else if (args[index] instanceof Date) {
                            delete = delete.replace(placeholder, "'" + args[index].toString() + "'");
                        }  else if (args[index] instanceof Time) {
                            delete = delete.replace(placeholder, "'" + args[index].toString() + "'");
                        } else if (args[index] instanceof byte[]) {
                            String hexString = bytesToHex((byte[]) args[index]);
                            delete = delete.replace(placeholder, "X'" + hexString + "'");
                        } else {
                            delete = delete.replace(placeholder, args[index].toString());
                        }
                    }
                }
            }
        }

        return delete;
    }

    public int delete(Connection connection) throws SQLException {
        Delete deleteAnnotation = method.getAnnotation(Delete.class);
        Map<String, Object> mapValues = new HashMap<>();
        String delete = deleteAnnotation.value();
        delete = delete.replace(" = ", "=");
        delete = delete.replace("=", " = ");

        if (args != null) {
            for (int index = 0; index < args.length; index++) {
                Param paramAnnotation = method.getParameters()[index].getAnnotation(Param.class);
                if (paramAnnotation != null) {
                    String paramName = paramAnnotation.value();
                    String placeholder = ":" + paramName;

                    if (delete.contains(placeholder)) {
                        delete = delete.replace(placeholder, "?");
                        mapValues.put(paramName, args[index]);
                    }
                }
            }
        }

        delete = String.format("%s;", delete);
        String[] parts = delete.split(" = \\?");

        int result;
        try (PreparedStatement preparedStatement = connection.prepareStatement(delete)) {
            for (int index = 0; index < parts.length - 1; index++) {
                String part = parts[index];
                String[] words = part.split(" ");
                String columnName = words[words.length - 1];
                preparedStatement.setObject(index + 1, mapValues.get(columnName));
            }

            result = preparedStatement.executeUpdate();
        }
        return result;
    }

    // Método auxiliar para converter byte[] em uma string hexadecimal
    private String bytesToHex(byte[] bytes) {
        StringBuilder sb = new StringBuilder();
        for (byte b : bytes) {
            sb.append(String.format("%02x", b));
        }
        return sb.toString();
    }

}
