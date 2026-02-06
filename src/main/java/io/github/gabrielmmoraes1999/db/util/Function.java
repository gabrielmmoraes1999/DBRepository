package io.github.gabrielmmoraes1999.db.util;

import io.github.gabrielmmoraes1999.db.annotation.*;

import java.lang.reflect.*;
import java.sql.*;
import java.util.*;

public class Function {

    public static Class<?> getClassList(Method method) {
        Class<?> listGenericType = null;
        Type returnType = method.getGenericReturnType();

        if (returnType instanceof ParameterizedType) {
            ParameterizedType parameterizedType = (ParameterizedType) returnType;
            Type type = parameterizedType.getActualTypeArguments()[0];

            if (type instanceof ParameterizedType) {
                ParameterizedType mapType = (ParameterizedType) type;
                listGenericType = (Class<?>) mapType.getRawType();
            } else {
                listGenericType = (Class<?>) type;
            }
        }

        return listGenericType;
    }

}
