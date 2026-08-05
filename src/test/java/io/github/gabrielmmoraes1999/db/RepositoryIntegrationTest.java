package io.github.gabrielmmoraes1999.db;

import io.github.gabrielmmoraes1999.db.entity.User;
import io.github.gabrielmmoraes1999.db.repository.UserRepository;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class RepositoryIntegrationTest {

    private Connection connection;
    private UserRepository repository;

    @BeforeEach
    void setUp() throws Exception {
        connection = DriverManager.getConnection("jdbc:h2:mem:dbrepository;MODE=MySQL;DB_CLOSE_DELAY=-1");
        connection.setAutoCommit(false);
        DataBase.setAutoCommit(true, connection);

        try (Statement statement = connection.createStatement()) {
            statement.execute("DROP TABLE IF EXISTS USERS");
            statement.execute("CREATE TABLE USERS (" +
                    "ID INT PRIMARY KEY, " +
                    "NAME VARCHAR(100), " +
                    "AGE INT)");
        }
        connection.commit();

        repository = Repository.createRepository(UserRepository.class, connection);
        repository.insert(new User(1, "Ana", 20));
        repository.insert(new User(2, "Bruno", 30));
        repository.insert(new User(3, "Ana", 40));
    }

    @AfterEach
    void tearDown() throws Exception {
        if (connection != null && !connection.isClosed()) {
            connection.close();
        }
    }

    @Test
    void crudFindByAndQuery() {
        User found = repository.findById(1);
        assertNotNull(found);
        assertEquals("Ana", found.getName());

        List<User> anas = repository.findByName("Ana");
        assertEquals(2, anas.size());

        List<User> adults = repository.findByAgeGreaterThan(25);
        assertEquals(2, adults.size());

        List<User> queried = repository.searchByName("Bruno");
        assertEquals(1, queried.size());
        assertEquals(2, queried.get(0).getId());

        List<User> all = repository.findAll();
        assertEquals(3, all.size());
    }

    @Test
    void inNotInAndJsonArray() {
        List<User> filtered = repository.findByNameAndIdIn("Ana", Arrays.asList(1, 3, 99));
        assertEquals(2, filtered.size());

        List<User> remaining = repository.findByIdNotIn(Arrays.asList(1, 2));
        assertEquals(1, remaining.size());
        assertEquals(3, remaining.get(0).getId());

        JSONArray jsonArray = repository.findByAgeGreaterThanOrderByNameAsc(18);
        assertEquals(3, jsonArray.length());
        assertEquals("Ana", jsonArray.getJSONObject(0).getString("NAME"));
        assertFalse(jsonArray.getJSONObject(0).isEmpty());

        JSONObject jsonObject = repository.findJsonById(2);
        assertEquals("Bruno", jsonObject.getString("NAME"));
    }

    @Test
    void countExistsAndDeleteBy() {
        assertEquals(2L, repository.countByAgeGreaterThan(25));
        assertTrue(repository.existsByName("Bruno"));
        assertFalse(repository.existsByName("Carla"));

        int deleted = repository.deleteByAgeLessThan(25);
        assertEquals(1, deleted);
        assertNull(repository.findById(1));
        assertEquals(2, repository.findAll().size());
    }

    @Test
    void saveUpdatesExistingEntity() {
        User user = repository.findById(2);
        user.setName("Bruno Silva");
        user.setAge(31);

        User saved = repository.save(user);
        assertEquals("Bruno Silva", saved.getName());
        assertEquals(31, saved.getAge());
    }
}
