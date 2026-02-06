package io.github.gabrielmmoraes1999.db.sql;

import io.github.gabrielmmoraes1999.db.annotation.*;
import io.github.gabrielmmoraes1999.db.util.Function;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.sql.*;
import java.util.*;

public class DQL {

    public static <T> List<T> findAll(Class<T> entityClass, Connection connection) {
        if (!entityClass.isAnnotationPresent(Table.class)) {
            throw new IllegalArgumentException("The class does not have the annotation @Table.");
        }

        List<T> entityList = new ArrayList<>();
        Table table = Objects.requireNonNull(entityClass.getAnnotation(Table.class));
        String sql = "SELECT * FROM " + table.name();

        try (Statement statement = connection.createStatement()) {
            for (Map<String, Object> row : Function.convertResultSetToMap(statement.executeQuery(sql))) {
                entityList.add(Function.getEntity(entityClass, row, connection));
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        return entityList;
    }

    public static <T, ID> T findById(Class<T> entityClass, ID id, Connection connection) throws NoSuchMethodException, InvocationTargetException, InstantiationException, IllegalAccessException, SQLException {
        if (!entityClass.isAnnotationPresent(Table.class)) {
            throw new IllegalArgumentException("The class does not have the annotation @Table.");
        }

        T entity = entityClass.getDeclaredConstructor().newInstance();
        int sizePrimatyKey = 0;

        List<Object> keys;
        if (id instanceof List) {
            keys = (List<Object>) id;
        } else {
            keys = Collections.singletonList(id);
        }

        for (Field field : entityClass.getDeclaredFields()) {
            if (!field.isAnnotationPresent(Column.class)) {
                continue;
            }

            if (field.isAnnotationPresent(PrimaryKey.class)) {
                field.setAccessible(true);
                field.set(entity, keys.get(sizePrimatyKey));
                sizePrimatyKey++;
            }
        }

        if (!Objects.equals(sizePrimatyKey, keys.size())) {
            throw new IllegalArgumentException("The amount PK invalid.");
        }

        return DQL.findById(entity, connection);
    }

    public static <T> T findById(T entity, Connection connection) throws InvocationTargetException, InstantiationException, NoSuchMethodException, IllegalAccessException, SQLException {
        @SuppressWarnings("unchecked")
        Class<T> entityClass = (Class<T>) entity.getClass();

        if (!entityClass.isAnnotationPresent(Table.class)) {
            throw new IllegalArgumentException("The class does not have the annotation @Table.");
        }

        Table table = entityClass.getAnnotation(Table.class);
        String tableName = table.name();

        StringJoiner selectClause = new StringJoiner(", ");
        StringJoiner whereClause = new StringJoiner(" AND ");
        List<Field> primaryKeyFields = new ArrayList<>();

        for (Field field : entityClass.getDeclaredFields()) {
            if (!field.isAnnotationPresent(Column.class)) {
                continue;
            }
            Column column = field.getAnnotation(Column.class);

            if (field.isAnnotationPresent(PrimaryKey.class)) {
                whereClause.add(String.format("%s = ?", column.name()));
                primaryKeyFields.add(field);

                field.setAccessible(true);
                if (Objects.isNull(field.get(entity))) {
                    throw new IllegalArgumentException("Valor do @PrimaryKey é nulo");
                }
            }

            selectClause.add(column.name());
        }

        if (primaryKeyFields.isEmpty()) {
            throw new IllegalArgumentException("Classe sem @PrimaryKey");
        }

        String sql = String.format("SELECT %s FROM %s WHERE %s", selectClause, tableName, whereClause);
        try (PreparedStatement preparedStatement = SQLUtils.getPreparedStatement(sql, entity, primaryKeyFields, connection);
             ResultSet rs = preparedStatement.executeQuery()) {

            if (rs.next()) {
                entity = entityClass.getDeclaredConstructor().newInstance();

                for (Field field : entityClass.getDeclaredFields()) {
                    if (!field.isAnnotationPresent(Column.class)) {
                        continue;
                    }

                    Column column = field.getAnnotation(Column.class);
                    field.setAccessible(true);
                    field.set(entity, rs.getObject(column.name()));
                }

                DQL.loadOneToOne(entity, connection);
                DQL.loadOneToMany(entity, connection);
                return entity;
            }
        }

        return null;
    }

    private static void loadOneToMany(Object parent, Connection connection) throws IllegalAccessException, SQLException, NoSuchMethodException, InvocationTargetException, InstantiationException {
        Class<?> parentClass = parent.getClass();

        for (Field field : parentClass.getDeclaredFields()) {
            if (!field.isAnnotationPresent(OneToMany.class)) {
                continue;
            }

            field.setAccessible(true);

            JoinColumns joinColumns = field.getAnnotation(JoinColumns.class);
            JoinColumn joinColumn = field.getAnnotation(JoinColumn.class);

            List<JoinColumn> joins = new ArrayList<>();

            if (joinColumns != null) {
                joins.addAll(Arrays.asList(joinColumns.value()));
            } else if (joinColumn != null) {
                joins.add(joinColumn);
            } else {
                throw new IllegalArgumentException("@OneToOne sem @JoinColumn(s): " + field.getName());
            }

            Class<?> childClass = SQLUtils.getCollectionGenericType(field);

            if (!childClass.isAnnotationPresent(Table.class)) {
                throw new IllegalArgumentException("Classe filha sem @Table");
            }

            Table table = childClass.getAnnotation(Table.class);
            String tableName = table.name();

            // SELECT colunas
            StringJoiner select = new StringJoiner(", ");
            for (Field f : childClass.getDeclaredFields()) {
                if (f.isAnnotationPresent(Column.class)) {
                    select.add(f.getAnnotation(Column.class).name());
                }
            }

            // WHERE com JoinColumns
            StringJoiner where = new StringJoiner(" AND ");
            List<Object> params = new ArrayList<>();

            for (JoinColumn jc : joins) {
                Field parentField = SQLUtils.findFieldByColumn(parentClass, jc.referencedColumnName());

                if (parentField == null) {
                    throw new RuntimeException("Campo não encontrado: " + jc.referencedColumnName());
                }

                parentField.setAccessible(true);
                Object value = parentField.get(parent);

                where.add(jc.name() + " = ?");
                params.add(value);
            }

            List<Object> children = new ArrayList<>();
            String sql = String.format("SELECT %s FROM %s WHERE %s", select, tableName, where);

            try (PreparedStatement ps = connection.prepareStatement(sql)) {
                for (int i = 0; i < params.size(); i++) {
                    ps.setObject(i + 1, params.get(i));
                }

                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        Object child = childClass.getDeclaredConstructor().newInstance();

                        for (Field f : childClass.getDeclaredFields()) {
                            if (!f.isAnnotationPresent(Column.class)) {
                                continue;
                            }

                            Column col = f.getAnnotation(Column.class);
                            f.setAccessible(true);
                            f.set(child, rs.getObject(col.name()));
                        }

                        children.add(child);
                    }
                }
            }

            field.set(parent, children);
        }
    }

    private static void loadOneToOne(Object parent, Connection connection) throws IllegalAccessException, SQLException, NoSuchMethodException, InvocationTargetException, InstantiationException {
        Class<?> parentClass = parent.getClass();

        for (Field field : parentClass.getDeclaredFields()) {
            if (!field.isAnnotationPresent(OneToOne.class)) {
                continue;
            }

            field.setAccessible(true);

            // Suporta @JoinColumn ou @JoinColumns
            JoinColumns joinColumns = field.getAnnotation(JoinColumns.class);
            JoinColumn joinColumn = field.getAnnotation(JoinColumn.class);

            List<JoinColumn> joins = new ArrayList<>();

            if (joinColumns != null) {
                joins.addAll(Arrays.asList(joinColumns.value()));
            } else if (joinColumn != null) {
                joins.add(joinColumn);
            } else {
                throw new IllegalArgumentException("@OneToOne sem @JoinColumn(s): " + field.getName());
            }

            Class<?> targetClass = field.getType();

            if (!targetClass.isAnnotationPresent(Table.class)) {
                throw new IllegalArgumentException("Classe OneToOne sem @Table: " + targetClass.getName());
            }

            Table table = targetClass.getAnnotation(Table.class);
            String tableName = table.name();

            // SELECT colunas
            StringJoiner select = new StringJoiner(", ");
            for (Field f : targetClass.getDeclaredFields()) {
                if (f.isAnnotationPresent(Column.class)) {
                    select.add(f.getAnnotation(Column.class).name());
                }
            }

            // WHERE
            StringJoiner where = new StringJoiner(" AND ");
            List<Object> params = new ArrayList<>();

            for (JoinColumn jc : joins) {
                Field parentField = SQLUtils.findFieldByColumn(
                        parentClass,
                        jc.referencedColumnName());

                if (parentField == null) {
                    throw new RuntimeException(
                            "Campo não encontrado: " + jc.referencedColumnName());
                }

                parentField.setAccessible(true);
                Object value = parentField.get(parent);

                where.add(jc.name() + " = ?");
                params.add(value);
            }

            String sql = String.format("SELECT %s FROM %s WHERE %s", select, tableName, where);
            try (PreparedStatement ps = connection.prepareStatement(sql)) {

                for (int i = 0; i < params.size(); i++) {
                    ps.setObject(i + 1, params.get(i));
                }

                try (ResultSet rs = ps.executeQuery()) {

                    if (rs.next()) {
                        Object target = targetClass
                                .getDeclaredConstructor()
                                .newInstance();

                        for (Field f : targetClass.getDeclaredFields()) {
                            if (!f.isAnnotationPresent(Column.class)) {
                                continue;
                            }

                            Column col = f.getAnnotation(Column.class);
                            f.setAccessible(true);
                            f.set(target, rs.getObject(col.name()));
                        }

                        field.set(parent, target);
                    } else {
                        field.set(parent, null);
                    }
                }
            }
        }
    }

}
