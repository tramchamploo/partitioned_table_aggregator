package util.tuple;

/**
 * Created by guohang.bao on 15-8-27.
 */
public class Tuple<T, U> {

    private T __1;

    private U __2;

    Tuple(T __1, U __2) {
        this.__1 = __1;
        this.__2 = __2;
    }

    public T _1() {
        return __1;
    }

    public U _2() {
        return __2;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Tuple<?, ?> tuple = (Tuple<?, ?>) o;

        if (__1 != null ? !__1.equals(tuple.__1) : tuple.__1 != null) return false;
        return !(__2 != null ? !__2.equals(tuple.__2) : tuple.__2 != null);

    }

    @Override
    public int hashCode() {
        int result = __1 != null ? __1.hashCode() : 0;
        result = 31 * result + (__2 != null ? __2.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "Tuple<" + __1 + ", " + __2 + '>';
    }
}
