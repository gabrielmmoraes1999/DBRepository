package org.example.database.sql;

import org.example.database.annotation.Delete;
import org.example.database.annotation.Param;
import org.example.database.annotation.Query;
import org.example.database.annotation.Update;

import java.lang.reflect.Method;

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
                        } else {
                            delete = delete.replace(placeholder, args[index].toString());
                        }
                    }
                }
            }
        }

        return delete;
    }

}
