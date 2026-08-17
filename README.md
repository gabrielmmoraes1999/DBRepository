# DBRepository [![MIT License](https://img.shields.io/github/license/gabrielmmoraes1999/DBRepository.svg) ](https://github.com/gabrielmmoraes1999/DBRepository/blob/main/LICENSE) [![Maven Central](https://img.shields.io/maven-central/v/io.github.gabrielmmoraes1999/DBRepository.svg?label=Maven%20Central)](https://central.sonatype.com/artifact/io.github.gabrielmmoraes1999/DBRepository)
Biblioteca Java para manipulação de banco de dados.

Importação da biblioteca:
- Maven :
```xml
<dependency>
    <groupId>io.github.gabrielmmoraes1999</groupId>
    <artifactId>DBRepository</artifactId>
    <version>2.1.3</version>
</dependency>
```

Veja a Wiki https://github.com/gabrielmmoraes1999/DBRepository/wiki, para ter um Tutorial Completo.

## Exemplo rápido

```java
@Table(name = "USERS")
public class User {
    @PrimaryKey
    @Column(name = "ID")
    private Integer id;

    @Column(name = "NAME")
    private String name;

    // getters/setters
}

public interface UserRepository extends DBRepository<User, Integer> {
    List<User> findByName(String name);

    long countByAgeGreaterThan(Integer age);

    @Query("SELECT * FROM USERS WHERE NAME = :name")
    List<User> searchByName(@Param("name") String name);
}

// Conexão global (adequada para apps single-thread / desktop)
DataBase.createConnection("jdbc:h2:mem:demo", "sa", "");
UserRepository repository = Repository.createRepository(UserRepository.class);

User user = new User();
user.setId(1);
user.setName("Ana");
repository.insert(user);

List<User> users = repository.findByName("Ana");
```

### Conexão e thread-safety

- **Sem pool:** `DataBase.createConnection(...)` guarda uma conexão JDBC estática. Use em cenários single-thread, ou passe uma `Connection` explícita em `Repository.createRepository(iface, connection)`.
- **Com pool:** configure um `HikariDataSource` via `ConnectionPoolManager.setHikariDataSource(...)`. Cada chamada do repositório obtém e devolve uma conexão do pool.
- O mapa interno de autocommit usa `ConcurrentHashMap`, mas a conexão global única ainda não é segura para uso concorrente sem sincronização externa.

________________________________________________________________________________________________

# Histórico de Versões

## v2.1.4 - 17/08/2026
- Corrigido erro de `auto-commit`.

## v2.1.3 - 05/08/2026
- Corrigido retorno `JSONArray` em consultas derivadas (`findBy*`).
- Corrigido binding e expansão de `IN` / `NOT IN` com múltiplos parâmetros.
- Habilitados métodos derivados `countBy*`, `existsBy*`, `deleteBy*` / `removeBy*`.
- Endurecida a gestão de conexão (`ConcurrentHashMap` e log em falhas de `disconnect`).
- Adicionados testes (JUnit 5 + H2) e pipeline CI.

## v2.1.2 - 16/06/2026
- Removido a Exception quando a chave primaria for nula.

## v2.1.1 - 12/06/2026
- Restaurado o suporte do retorno de entidade quando usado da anotação `Query`.

## v2.1.0 - 16/02/2026
- Adicionado suporte a metodos com `in` com argumento `List<?>`.

## v2.0.3 - 15/02/2026
- Correção bug insert ou update quando entidade tem a anotações `OneToMany` e `OneToOne`.

## v2.0.2 - 13/02/2026
- Melhoria na performance de entidade

## v2.0.1 - 08/02/2026
- Correção bug em campos do tipo Enum

## v2.0.0 - 06/02/2026
- Reorganização das classes

## v1.5.5 - 03/01/2026
- Suporte a returno de outros tipos de entidades.

## v1.5.4 - 08/11/2025
- Corrigido o fechamento das conexoes quando usado o pool de conexão.

## v1.5.3 - 15/10/2025
- Suporte OneToMany para campos List e Set.

## v1.5.2 - 28/09/2025
- Ajuste no comportamento do OneToOne.

## v1.5.1 - 05/08/2025
- Corrigido bug result close.

## v1.5.0 - 31/07/2025
- Adicionado comportamento para anotações `OneToMany` e `OneToOne`.
- Adicionado o autoload chaves estrangeiras nas tabelas ao usar essas anotações.
- Adicionado as anotações `JoinColumn` e `JoinColumns` caso queira informar manualmente as chaves estrangeiras.

## v1.4.2 - 24/07/2025
- Melhoria logica de commit `insertAll` e `saveAll`.

## v1.4.1 - 23/07/2025
- Melhoria logica de commit.

## v1.4.0 - 20/07/2025
- Adicionado suporte a ConnectionPoolManager com `HikariCP`.

## v1.3.0 - 08/07/2025
- Adicionado controle de autocommit pela lib.
- Implementados métodos `insertAll` e `saveAll`.
- Alterado o método padrão de `delete` para `deleteById`.

## v1.2.2 - 03/07/2025
- Corrigido bug no insert com campos nulos.

## v1.2.1 - 30/06/2025
- Adicionado suporte a tipo de class Enum.

## v1.2.0 - 12/06/2025
- Adicionado suporte return JSONObject e JSONArray.

## v1.1.2 - 22/05/2025
- Removido a opção entity da anotação 'Query', agora será capturado automaticamente pelo retorno da função.

## v1.1.1 - 21/04/2025
- Alterado alguns para não usar mais o 'setObject' para função interna 'setPreparedStatement'.

## v1.1.0 - 19/04/2025
- Adicionado opção de select sem uso da anotação 'Query'.

## v1.0.2 - 24/12/2024
- Adicionado propriedade da conexão

## v1.0 - 19/10/2024
- Versão inicial
