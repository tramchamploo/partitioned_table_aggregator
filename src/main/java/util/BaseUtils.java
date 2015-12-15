package util;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import org.joda.time.DateTime;
import org.springframework.jdbc.core.RowMapper;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

public class BaseUtils {

    public static <U, T extends Iterable<U>, K> Map<K, List<U>> groupBy(T iterable, Function<U, K> by) {
        Map<K, List<U>> ret = new HashMap<K, List<U>>();
        for (U entry : iterable) {
            K key = by.apply(entry);
            if (!ret.containsKey(key)) ret.put(key, new LinkedList<U>());
            List<U> groupEntries = ret.get(key);
            groupEntries.add(entry);
        }
        return ret;
    }


    public static <T, U extends Iterable<T>> Map<T, U> level(U all, Predicate<T> isFirstLevel, Function<T, U> children) {
        Iterable<T> firstLevels = Iterables.filter(all, isFirstLevel);
        Map<T, U> ret = new HashMap<T, U>();
        for (T fl : firstLevels) {
            ret.put(fl, children.apply(fl));
        }
        return ret;
    }

    public static <B, A extends B> B reduceLeft(Iterable<A> target, Functions.Function2<B, A, B> reducer, B initial) {
        Iterator<A> iter = target.iterator();
        if (!iter.hasNext()) return null;
        else {
            B l = initial == null ? iter.next() : initial;
            if (!iter.hasNext()) return l;
            else {
                A r = iter.next();
                l = reducer.apply(l, r);
                while (iter.hasNext()) {
                    r = iter.next();
                    l = reducer.apply(l, r);
                }
                return l;
            }
        }
    }

    public static <B, A extends B> B reduceLeft(Iterable<A> target, Functions.Function2<B, A, B> reducer) {
        return reduceLeft(target, reducer, null);
    }

    public static <T> BeanBuilder<T> createBeanBuilder(Class<T> targetCls) {
        return new BeanBuilder<T>(targetCls);
    }

    public static <T> BeanBuilder<T> createBeanBuilder(Class<T> targetCls, Iterable<String> fieldNames) {
        return new BeanBuilder<T>(targetCls, fieldNames);
    }

    public static <T> RowMapper<T> rowMapper(Class<T> targetCls, Iterable<String> fieldNames) {
        final BeanBuilder<T> builder = BaseUtils.createBeanBuilder(targetCls, fieldNames);
        return new RowMapper() {

            public T mapRow(ResultSet resultSet, int i) throws SQLException {
                try {
                    return builder.build(resultSet);
                } catch (Exception e) {
                    e.printStackTrace();
                }
                return null;
            }
        };
    }

    public static class BeanBuilder<T> {
        private Class<T> targetCls;
        private Iterable<String> fn;
        private Iterable<String> an;

        private BeanBuilder(Class<T> targetCls) {
            this.targetCls = targetCls;
        }

        private BeanBuilder(Class<T> targetCls, Iterable<String> fieldNames) {
            this.targetCls = targetCls;
            this.fn = fieldNames;
            this.an = defaultAttrNames(fieldNames);
        }

        private BeanBuilder(Class<T> targetCls, Iterable<String> fieldNames, Iterable<String> attrNames) {
            this.targetCls = targetCls;
            this.fn = fieldNames;
            this.an = attrNames;
        }

        private Iterable<String> defaultAttrNames(Iterable<String> fieldNames) {
            return Iterables.transform(fieldNames, new Function<String, String>() {
                public String apply(String input) {
                    int underLineIdx;
                    if ((underLineIdx = input.indexOf("_")) > -1) {
                        return apply(input.substring(0, underLineIdx) + input.substring(underLineIdx + 1, underLineIdx + 2).toUpperCase() + input.substring(underLineIdx + 2));
                    } else {
                        return input;
                    }
                }
            });
        }

        public T build(Iterable<String> fieldNames, ResultSet rs) throws IllegalAccessException, InvocationTargetException, InstantiationException, SQLException, NoSuchMethodException, NoSuchFieldException {
            return build(fieldNames, defaultAttrNames(fieldNames), rs);
        }

        public T build(ResultSet rs) throws IllegalAccessException, InvocationTargetException, InstantiationException, SQLException, NoSuchMethodException, NoSuchFieldException {
            return build(this.fn, this.an, rs);
        }

        public T build(Iterable<String> fieldNames, Iterable<String> attrNames, ResultSet rs) throws IllegalAccessException, InstantiationException, NoSuchFieldException, NoSuchMethodException, SQLException, InvocationTargetException {
            int fnSize = Iterables.size(fieldNames);
            if (fnSize != Iterables.size(attrNames)) throw new RuntimeException("sizes don't match!");
            T instance = targetCls.newInstance();
            for (int i = 0; i < fnSize; i++) {
                String fn = Iterables.get(fieldNames, i);
                String an = Iterables.get(attrNames, i);
                Field field = targetCls.getDeclaredField(an);
                Class<?> fieldType = field.getType();
                Method setter = targetCls.getDeclaredMethod("set" + an.substring(0, 1).toUpperCase() + an.substring(1), fieldType);
                setter.setAccessible(true);
                setter.invoke(instance, invokeGetter(fieldType.getName(), rs, fn));
            }
            return instance;
        }

        private Object invokeGetter(String typeName, ResultSet rs, String fieldName) throws SQLException {
            if (typeName.equals("java.lang.String")) return rs.getString(fieldName);
            else if (typeName.equals("java.lang.Boolean") || typeName.equals("boolean"))
                return rs.getInt(fieldName) > 0;
            else if (typeName.equals("java.lang.Integer") || typeName.equals("int")) return rs.getInt(fieldName);
            else if (typeName.equals("java.lang.Long") || typeName.equals("long")) return rs.getLong(fieldName);
            else if (typeName.equals("java.sql.Timestamp")) return rs.getTimestamp(fieldName);
            else if (typeName.equals("org.joda.time.DateTime")) return new DateTime(rs.getTimestamp(fieldName));
            else return rs.getObject(fieldName);
        }
    }

    public static <T> List<T> limitedList(List<T> src, int limit) {
        if (src == null || limit <= 0) return src;
        return Lists.newArrayList(src.subList(0, Math.min(limit, src.size())));
    }

    public static boolean noArgs(Object... args) {
        return args.length == 0;
    }
}
