package io.github.gabrielmmoraes1999.db.sql;

import io.github.gabrielmmoraes1999.db.annotation.*;

import java.lang.reflect.Field;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.math.BigDecimal;
import java.sql.*;
import java.sql.Date;
import java.util.*;

public class DML {

    public static int insert(Object entity, Connection connection) throws SQLException, IllegalAccessException {
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

            if (Objects.nonNull(field.get(entity))) {
                columns.add(column.name());
                values.add("?");
                fields.add(field);
            }
        }

        String sql = String.format("INSERT INTO %s (%s) VALUES (%s)", tableName, columns, values);
        return DML.prepareStatement(sql, entity, fields, connection);
    }

    public static int insertCascade(Object entity, Connection connection) throws SQLException, IllegalAccessException {
        Class<?> clazz = entity.getClass();
        int result = insert(entity, connection);

        for (Field field : clazz.getDeclaredFields()) {
            if (!field.isAnnotationPresent(JoinColumns.class)) {
                continue;
            }

            if (field.isAnnotationPresent(OneToMany.class)) {
                field.setAccessible(true);
                Object value = field.get(entity);

                if (value == null) {
                    continue;
                }

                if (!(value instanceof Collection)) {
                    continue;
                }

                for (Object child : (Collection<?>) value) {
                    copyJoinColumns(entity, child, field);
                    result = result + insert(child, connection);
                }
            }
        }

        return result;
    }

    public static void update(Object entity, Connection connection) throws SQLException, IllegalAccessException {
        Class<?> clazz = entity.getClass();

        if (!clazz.isAnnotationPresent(Table.class)) {
            throw new IllegalArgumentException("Classe sem @Table");
        }

        Table table = clazz.getAnnotation(Table.class);
        String tableName = table.name();

        StringJoiner setClause = new StringJoiner(", ");
        StringJoiner whereClause = new StringJoiner(", ");
        List<Field> updateFields = new ArrayList<>();
        List<Field> primaryKeyFields = new ArrayList<>();
        List<Field> fields = new ArrayList<>();

        for (Field field : clazz.getDeclaredFields()) {
            field.setAccessible(true);

            if (!field.isAnnotationPresent(Column.class)) {
                continue;
            }

            Column column = field.getAnnotation(Column.class);
            if (field.isAnnotationPresent(PrimaryKey.class)) {
                whereClause.add(String.format("%s = ?", column.name()));
                primaryKeyFields.add(field);
            } else {
                setClause.add(String.format("%s = ?", column.name()));
                updateFields.add(field);
            }
        }

        if (primaryKeyFields.isEmpty()) {
            throw new IllegalArgumentException("Classe sem @PrimaryKey");
        }

        for (Field field : primaryKeyFields) {
            field.setAccessible(true);

            if (Objects.isNull(field.get(entity))) {
                throw new IllegalArgumentException("Valor do @PrimaryKey é nulo");
            }
        }

        fields.addAll(updateFields);
        fields.addAll(primaryKeyFields);
        String sql = String.format("UPDATE %s SET %s WHERE %s", tableName, setClause, whereClause);
        DML.prepareStatement(sql, entity, fields, connection);
    }

    public static void updateCascade(Object entity, Connection connection) throws SQLException, IllegalAccessException {
        update(entity, connection);
        Class<?> clazz = entity.getClass();

        // 2. Processa relações @OneToMany
        for (Field field : clazz.getDeclaredFields()) {

            if (!field.isAnnotationPresent(OneToMany.class)) {
                continue;
            }

            field.setAccessible(true);
            Object value = field.get(entity);

            // 3. Deleta filhos sempre (mesmo se lista for null ou vazia)
            deleteChildren(entity, field, connection);

            if (value == null) {
                continue;
            }

            if (!(value instanceof Collection)) {
                continue;
            }

            Collection<?> collection = (Collection<?>) value;

            // 4. Insere novamente os filhos
            for (Object child : collection) {
                copyJoinColumns(entity, child, field);
                insert(child, connection);
            }
        }
    }

    public static void delete(Object entity, Connection connection) throws SQLException, IllegalAccessException {
        Class<?> clazz = entity.getClass();

        if (!clazz.isAnnotationPresent(Table.class)) {
            throw new IllegalArgumentException("Classe sem @Table");
        }

        Table table = clazz.getAnnotation(Table.class);
        String tableName = table.name();

        StringJoiner whereClause = new StringJoiner(", ");
        List<Field> fields = new ArrayList<>();

        for (Field field : clazz.getDeclaredFields()) {
            field.setAccessible(true);

            if (!field.isAnnotationPresent(Column.class)) {
                continue;
            }

            Column column = field.getAnnotation(Column.class);
            if (field.isAnnotationPresent(PrimaryKey.class)) {
                whereClause.add(String.format("%s = ?", column.name()));
                fields.add(field);
            }
        }

        if (fields.isEmpty()) {
            throw new IllegalArgumentException("Classe sem @PrimaryKey");
        }

        for (Field field : fields) {
            field.setAccessible(true);

            if (Objects.isNull(field.get(entity))) {
                throw new IllegalArgumentException("Valor do @PrimaryKey é nulo");
            }
        }

        String sql = String.format("UPDATE %s WHERE %s", tableName, whereClause);
        DML.prepareStatement(sql, entity, fields, connection);
    }

    private static void deleteChildren(Object parent, Field oneToManyField, Connection connection) throws SQLException, IllegalAccessException {

        JoinColumns joinColumns = oneToManyField.getAnnotation(JoinColumns.class);
        if (joinColumns == null) {
            return;
        }

        // Descobre tabela do filho
        Class<?> childClass = getCollectionGenericType(oneToManyField);
        if (!childClass.isAnnotationPresent(Table.class)) {
            throw new RuntimeException("Classe filha sem @Table");
        }

        Table table = childClass.getAnnotation(Table.class);
        String tableName = table.name();

        StringJoiner where = new StringJoiner(" AND ");
        List<Object> params = new ArrayList<>();

        for (JoinColumn joinColumn : joinColumns.value()) {

            Field parentField = findFieldByColumn(
                    parent.getClass(),
                    joinColumn.referencedColumnName());

            if (parentField == null) {
                throw new RuntimeException(
                        "Campo não encontrado: " + joinColumn.referencedColumnName());
            }

            parentField.setAccessible(true);

            where.add(joinColumn.name() + " = ?");
            params.add(parentField.get(parent));
        }

        String sql = "DELETE FROM " + tableName + " WHERE " + where;

        DML.execute(sql, params, connection);
    }

    private static int prepareStatement(String sql, Object entity, List<Field> fields, Connection connection) throws SQLException, IllegalAccessException {
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
            return preparedStatement.executeUpdate();
        }
    }

    private static Class<?> getCollectionGenericType(Field field) {
        Type type = field.getGenericType();

        if (type instanceof ParameterizedType) {
            ParameterizedType pt = (ParameterizedType) type;
            Type[] args = pt.getActualTypeArguments();
            if (args.length > 0 && args[0] instanceof Class) {
                return (Class<?>) args[0];
            }
        }

        throw new RuntimeException(
                "Não foi possível descobrir tipo genérico de " + field.getName());
    }


    private static void copyJoinColumns(Object parent, Object child, Field oneToManyField) throws IllegalAccessException {
        JoinColumns joinColumns = oneToManyField.getAnnotation(JoinColumns.class);
        if (joinColumns == null) return;

        Class<?> parentClass = parent.getClass();
        Class<?> childClass = child.getClass();

        for (JoinColumn joinColumn : joinColumns.value()) {
            String referencedColumn = joinColumn.referencedColumnName();
            Field parentField = findFieldByColumn(parentClass, referencedColumn);
            Field childField = findFieldByColumn(childClass, joinColumn.name());

            if (parentField == null) {
                throw new RuntimeException("JoinColumn inválido: '" + joinColumn.name() + "' não existente na tabela pai.");
            }

            if (childField == null) {
                throw new RuntimeException("JoinColumn inválido: '" + joinColumn.name() + "' não existente na tabela filha.");
            }

            parentField.setAccessible(true);
            childField.setAccessible(true);

            Object value = parentField.get(parent);
            childField.set(child, value);
        }
    }

    private static Field findFieldByColumn(Class<?> clazz, String columnName) {
        for (Field field : clazz.getDeclaredFields()) {
            Column column = field.getAnnotation(Column.class);
            if (column != null && column.name().equals(columnName)) {
                return field;
            }
        }
        return null;
    }

}
