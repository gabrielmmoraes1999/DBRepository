package io.github.gabrielmmoraes1999.db.sql;

import io.github.gabrielmmoraes1999.db.annotation.*;

import java.lang.reflect.Field;
import java.sql.*;
import java.util.*;

public class DML {

    public static int insert(Object entity, Connection connection) throws SQLException, IllegalAccessException {
        Class<?> entityClass = entity.getClass();

        if (!entityClass.isAnnotationPresent(Table.class)) {
            throw new IllegalArgumentException("The class does not have the annotation @Table");
        }

        Table table = entityClass.getAnnotation(Table.class);
        String tableName = table.name();

        StringJoiner columns = new StringJoiner(", ");
        StringJoiner values = new StringJoiner(", ");
        List<Field> fields = new ArrayList<>();

        for (Field field : entityClass.getDeclaredFields()) {
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
        return SQLUtils.preparedStatement(sql, entity, fields, connection);
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
                    SQLUtils.copyJoinColumns(entity, child, field);
                    result = result + insert(child, connection);
                }
            }
        }

        return result;
    }

    public static int update(Object entity, Connection connection) throws SQLException, IllegalAccessException {
        Class<?> entityClass = entity.getClass();

        if (!entityClass.isAnnotationPresent(Table.class)) {
            throw new IllegalArgumentException("The class does not have the annotation @Table");
        }

        Table table = entityClass.getAnnotation(Table.class);
        String tableName = table.name();

        StringJoiner setClause = new StringJoiner(", ");
        StringJoiner whereClause = new StringJoiner(" AND ");
        List<Field> updateFields = new ArrayList<>();
        List<Field> primaryKeyFields = new ArrayList<>();
        List<Field> fields = new ArrayList<>();

        for (Field field : entityClass.getDeclaredFields()) {
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
        return SQLUtils.preparedStatement(sql, entity, fields, connection);
    }

    public static int updateCascade(Object entity, Connection connection) throws SQLException, IllegalAccessException {
        int result = update(entity, connection);
        Class<?> entityClass = entity.getClass();

        for (Field field : entityClass.getDeclaredFields()) {
            if (!field.isAnnotationPresent(OneToMany.class)) {
                continue;
            }

            field.setAccessible(true);
            Object value = field.get(entity);

            result = result + DML.deleteChildren(entity, field, connection);

            if (value == null) {
                continue;
            }

            if (!(value instanceof Collection)) {
                continue;
            }

            for (Object child : (Collection<?>) value) {
                SQLUtils.copyJoinColumns(entity, child, field);
                result = result + insert(child, connection);
            }
        }

        return result;
    }

    public static <ID> int deleteById(ID id, Object entity, Connection connection) throws SQLException, IllegalAccessException {
        Class<?> entityClass = entity.getClass();

        if (!entityClass.isAnnotationPresent(Table.class)) {
            throw new IllegalArgumentException("The class does not have the annotation @Table");
        }

        Table table = entityClass.getAnnotation(Table.class);
        String tableName = table.name();

        StringJoiner whereClause = new StringJoiner(" AND ");
        List<Field> fields = new ArrayList<>();

        for (Field field : entityClass.getDeclaredFields()) {
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

        if (id instanceof List) {
            @SuppressWarnings("unchecked")
            List<Object> keys = (List<Object>) id;

            if (keys.size() != fields.size()) {
                throw new IllegalArgumentException("The amount PK invalid.");
            }

            for (Field field : fields) {
                field.setAccessible(true);
                int index = fields.indexOf(field);
                field.set(entity, keys.get(index));
            }
        } else {
            for (Field field : fields) {
                field.setAccessible(true);
                field.set(entity, id);
            }
        }

        String sql = String.format("DELETE %s WHERE %s", tableName, whereClause);
        return SQLUtils.preparedStatement(sql, entity, fields, connection);
    }

    protected static int deleteChildren(Object parent, Field oneToManyField, Connection connection) throws SQLException, IllegalAccessException {
        JoinColumns joinColumns = oneToManyField.getAnnotation(JoinColumns.class);
        if (joinColumns == null) {
            return 0;
        }

        Class<?> childClass = SQLUtils.getCollectionGenericType(oneToManyField);
        if (!childClass.isAnnotationPresent(Table.class)) {
            throw new RuntimeException("Classe filha sem @Table");
        }

        Table table = childClass.getAnnotation(Table.class);
        String tableName = table.name();

        StringJoiner whereClause = new StringJoiner(" AND ");
        List<Field> fields = new ArrayList<>();

        for (JoinColumn joinColumn : joinColumns.value()) {
            Field parentField = SQLUtils.findFieldByColumn(parent.getClass(), joinColumn.referencedColumnName());

            if (parentField == null) {
                throw new RuntimeException("Campo não encontrado: " + joinColumn.referencedColumnName());
            }

            parentField.setAccessible(true);
            whereClause.add(joinColumn.name() + " = ?");
            fields.add(parentField);
        }

        String sql = String.format("DELETE FROM %s WHERE %s", tableName, whereClause);
        return SQLUtils.preparedStatement(sql, childClass, fields, connection);
    }

}
