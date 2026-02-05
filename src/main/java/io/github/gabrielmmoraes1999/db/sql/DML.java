package io.github.gabrielmmoraes1999.db.sql;

import io.github.gabrielmmoraes1999.db.annotation.Column;
import io.github.gabrielmmoraes1999.db.annotation.Table;

import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.sql.*;
import java.sql.Date;
import java.util.*;

public class DML {

    public static void insert(Object entity, Connection connection) throws SQLException {
        Class<?> clazz = entity.getClass();

        if (!clazz.isAnnotationPresent(Table.class)) {
            throw new IllegalArgumentException("Classe sem @Table");
        }

        Table table = clazz.getAnnotation(Table.class);
        String tableName = table.name();

        StringJoiner columns = new StringJoiner(", ");
        StringJoiner values = new StringJoiner(", ");
        List<Field> fields = new ArrayList<>();

        for (Field field : clazz.getDeclaredFields()) {
            if (!field.isAnnotationPresent(Column.class)) {
                continue;
            }

            Column column = field.getAnnotation(Column.class);
            field.setAccessible(true);

            try {
                if (Objects.nonNull(field.get(entity))) {
                    columns.add(column.name());
                    values.add("?");
                    fields.add(field);
                }
            } catch (IllegalAccessException e) {
                throw new RuntimeException(e);
            }
        }

        String sql = String.format("INSERT INTO %s (%s) VALUES (%s)", tableName, columns, values);
        try (PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
            for (Field field : fields) {
                field.setAccessible(true);
                int position = fields.indexOf(field) + 1;
                Class<?> classType = field.getType();
                Object value = field.get(entity);

                if (classType == Integer.class) {
                    preparedStatement.setInt(position, (Integer) value);
                } else if (classType == Double.class) {
                    preparedStatement.setDouble(position, (Double) value);
                } else if (classType == BigDecimal.class) {
                    preparedStatement.setBigDecimal(position, (BigDecimal) value);
                } else if (classType == Boolean.class) {
                    preparedStatement.setBoolean(position, (Boolean) value);
                } else if (classType == String.class) {
                    preparedStatement.setString(position, (String) value);
                } else if (classType == java.sql.Date.class) {
                    preparedStatement.setDate(position, (Date) value);
                } else if (classType == Timestamp.class) {
                    preparedStatement.setTimestamp(position, (Timestamp) value);
                } else if (classType == Time.class) {
                    preparedStatement.setTime(position, (Time) value);
                } else if (classType == byte[].class) {
                    preparedStatement.setBytes(position, (byte[]) value);
                } else if (classType.isEnum()) {
                    preparedStatement.setInt(position, ((Enum<?>) value).ordinal());
                } else {
                    preparedStatement.setObject(position, value);
                }
            }
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        }

    }

}
