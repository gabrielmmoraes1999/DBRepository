package br.com.onges.database.sql;

import br.com.onges.database.annotation.Delete;
import br.com.onges.database.annotation.Param;
import br.com.onges.database.annotation.Query;
import br.com.onges.database.annotation.Update;

import java.lang.reflect.Method;
import java.sql.*;

public class QuerySQL {

    private final Method method;
    private final Object[] args;

    public QuerySQL(Method method, Object[] args) {
        this.method = method;
        this.args = args;
    }

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

    public String getDelete() {
        Delete updateAnnotation = method.getAnnotation(Delete.class);
        String delete = updateAnnotation.value();

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

    // Método auxiliar para converter byte[] em uma string hexadecimal
    private String bytesToHex(byte[] bytes) {
        StringBuilder sb = new StringBuilder();
        for (byte b : bytes) {
            sb.append(String.format("%02x", b));
        }
        return sb.toString();
    }

}
