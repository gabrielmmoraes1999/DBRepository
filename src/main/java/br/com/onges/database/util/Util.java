package br.com.onges.database.util;

import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;

public class Util {

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

    public static Class<?> getWrapperClass(Class<?> primitiveType) {
        if (primitiveType == int.class) return Integer.class;
        if (primitiveType == long.class) return Long.class;
        if (primitiveType == double.class) return Double.class;
        if (primitiveType == float.class) return Float.class;
        if (primitiveType == boolean.class) return Boolean.class;
        if (primitiveType == char.class) return Character.class;
        if (primitiveType == byte.class) return Byte.class;
        if (primitiveType == short.class) return Short.class;
        return primitiveType; // No caso de outros tipos
    }
}
