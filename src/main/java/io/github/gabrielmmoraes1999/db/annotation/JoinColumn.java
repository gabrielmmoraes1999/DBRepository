package io.github.gabrielmmoraes1999.db.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.FIELD)
public @interface JoinColumn {
    /** Nome da coluna FK na tabela filha */
    String name();

    /** Nome da coluna na tabela pai que a FK referencia (se vazio, usa a PK do pai) */
    String referencedColumnName() default "";

    boolean nullable() default true;
    boolean insertable() default true;
    boolean updatable() default true;
}

