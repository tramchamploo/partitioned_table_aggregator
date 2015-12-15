package util.tuple;

/**
 * Created by guohang.bao on 15-8-27.
 */
public class Tuples {

    public static <T, U> Tuple<T, U> of(T _1, U _2) {
        return new Tuple<T, U>(_1, _2);
    }

}
