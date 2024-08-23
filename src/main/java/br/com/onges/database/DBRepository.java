package br.com.onges.database;

import java.util.List;

public interface DBRepository<T, ID> {

    T save(T entity);

    T findById(ID id);

    List<T> findAll();

    Integer delete(ID id);
}
