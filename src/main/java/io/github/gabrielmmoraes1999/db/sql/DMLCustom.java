package io.github.gabrielmmoraes1999.db.sql;

import io.github.gabrielmmoraes1999.db.annotation.Delete;
import io.github.gabrielmmoraes1999.db.annotation.Param;
import io.github.gabrielmmoraes1999.db.annotation.Update;
import io.github.gabrielmmoraes1999.db.parse.SqlTemplate;

import java.lang.reflect.Method;
import java.lang.reflect.Parameter;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

public class DMLCustom {

    public static int update(Method method, Object[] args, Connection connection) throws SQLException {
        Update updateAnnotation = method.getAnnotation(Update.class);
        Parameter[] parameters = method.getParameters();
        SqlTemplate sqlTemplate = SqlTemplate.of(updateAnnotation.value());
        int result;

        if (args != null) {
            for (int index = 0; index < args.length; index++) {
                Param paramAnnotation = parameters[index].getAnnotation(Param.class);

                if (paramAnnotation != null) {
                    String paramName = paramAnnotation.value();
                    sqlTemplate.set(paramName, args[index]);
                }
            }
        }

        try (PreparedStatement preparedStatement = connection.prepareStatement(sqlTemplate.getSql())) {
            List<Object> bindValues = sqlTemplate.getBindValues();

            for (Object object : bindValues) {
                int index = bindValues.indexOf(object);
                SQLUtils.setPreparedStatement(preparedStatement, index + 1, object);
            }

            result = preparedStatement.executeUpdate();
        }

        return result;
    }

    public static int delete(Method method, Object[] args, Connection connection) throws SQLException {
        Delete deleteAnnotation = method.getAnnotation(Delete.class);
        Parameter[] parameters = method.getParameters();
        SqlTemplate sqlTemplate = SqlTemplate.of(deleteAnnotation.value());
        int result;

        if (args != null) {
            for (int index = 0; index < args.length; index++) {
                Param paramAnnotation = parameters[index].getAnnotation(Param.class);

                if (paramAnnotation != null) {
                    String paramName = paramAnnotation.value();
                    sqlTemplate.set(paramName, args[index]);
                }
            }
        }

        try (PreparedStatement preparedStatement = connection.prepareStatement(sqlTemplate.getSql())) {
            List<Object> bindValues = sqlTemplate.getBindValues();

            for (Object object : bindValues) {
                int index = bindValues.indexOf(object);
                SQLUtils.setPreparedStatement(preparedStatement, index + 1, object);
            }

            result = preparedStatement.executeUpdate();
        }

        return result;
    }

}
