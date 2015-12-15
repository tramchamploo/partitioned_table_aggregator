package function;

import net.sf.jsqlparser.expression.Function;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.List;

/**
 * Created by guohang.bao on 15/10/29.
 */
public interface FunctionExecutor<T, R> {

    R execute(Function f, String colName, List<T> group);

    static interface ValueHolder<V> {

        V get();

        void set(V val);
    }

    static class ObjectHolder implements ValueHolder<Object> {

        Object value = null;

        public Object get() {
            return value;
        }

        public void set(Object val) {
            value = val;
        }
    }

    static class IntHolder implements ValueHolder<Integer> {

        Integer value = null;

        public Integer get() {
            return value;
        }

        public void set(Integer val) {
            value = val;
        }
    }

    static class DoubleHolder implements ValueHolder<Double> {

        Double value = null;

        public Double get() {
            return value;
        }

        public void set(Double val) {
            value = val;
        }
    }

    static class LongHolder implements ValueHolder<Long> {

        Long value = null;

        public Long get() {
            return value;
        }

        public void set(Long val) {
            value = val;
        }
    }

    static class BigDecimalHolder implements ValueHolder<BigDecimal> {

        BigDecimal value = null;

        public BigDecimal get() {
            return value;
        }

        public void set(BigDecimal val) {
            value = val;
        }
    }

    static class ClassHolder implements ValueHolder<Class<?>> {

        Class<?> value = null;

        public Class<?> get() {
            return value;
        }

        public void set(Class<?> val) {
            value = val;
        }
    }

    static class TimestampHolder implements ValueHolder<Timestamp> {

        Timestamp value = null;

        public Timestamp get() {
            return value;
        }

        public void set(Timestamp val) {
            value = val;
        }
    }
}
