package org.example.database;

import java.util.List;

public interface DBRepository<T, ID> {

    T findById(ID id);

    List<T> findAll();

    T save(T entity);

    Integer delete(ID id);
}
