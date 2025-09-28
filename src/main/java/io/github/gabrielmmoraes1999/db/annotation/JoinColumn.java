package io.github.gabrielmmoraes1999.db.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.FIELD)
public @interface JoinColumn {
    String name();

    String referencedColumnName() default "";

    boolean nullable() default true;
    boolean insertable() default true;
    boolean updatable() default true;
}

