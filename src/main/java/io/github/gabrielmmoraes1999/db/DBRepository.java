package io.github.gabrielmmoraes1999.db;

import java.util.List;

public interface DBRepository<T, ID> {

    Integer insert(T entity);

    Integer update(T entity);

    T save(T entity);

    T findById(ID id);

    List<T> findAll();

    Integer delete(ID id);
}
