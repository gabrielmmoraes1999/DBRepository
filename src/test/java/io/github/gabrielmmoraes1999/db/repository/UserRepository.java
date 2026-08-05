package io.github.gabrielmmoraes1999.db.repository;

import io.github.gabrielmmoraes1999.db.DBRepository;
import io.github.gabrielmmoraes1999.db.annotation.Param;
import io.github.gabrielmmoraes1999.db.annotation.Query;
import io.github.gabrielmmoraes1999.db.entity.User;
import org.json.JSONArray;
import org.json.JSONObject;

import java.util.List;

public interface UserRepository extends DBRepository<User, Integer> {

    List<User> findByName(String name);

    List<User> findByAgeGreaterThan(Integer age);

    List<User> findByNameAndIdIn(String name, List<Integer> ids);

    List<User> findByIdNotIn(List<Integer> ids);

    JSONArray findByAgeGreaterThanOrderByNameAsc(Integer age);

    @Query("SELECT ID, NAME, AGE FROM USERS WHERE ID = :id")
    JSONObject findJsonById(@Param("id") Integer id);

    long countByAgeGreaterThan(Integer age);

    boolean existsByName(String name);

    int deleteByAgeLessThan(Integer age);

    @Query("SELECT * FROM USERS WHERE NAME = :name")
    List<User> searchByName(@Param("name") String name);
}
