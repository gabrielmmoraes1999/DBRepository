package io.github.gabrielmmoraes1999.db;

import java.util.List;

public interface DBRepository<T, ID> {

    Integer insert(T entity);

    Integer insertAll(List<T> entityList);

    Integer update(T entity);

    T save(T entity);

    List<T> saveAll(List<T> entityList);

    T findById(ID id);

    List<T> findAll();

    Integer deleteById(ID id);

}
