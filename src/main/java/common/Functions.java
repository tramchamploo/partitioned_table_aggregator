package common;


/**
 * Created by guohang.bao on 14-7-16.
 */

public class Functions {

    public interface Function0<T> {
        T apply();
    }

    public interface Function2<F, G, T> {
        T apply(F arg1, G arg2);
    }

    public interface Function1<A, B> {
        B apply(A arg);
    }

    public static <A, B, G> Function1<G, B> compose(final Function1<A, B> f, final Function1<G, A> g) {
        return new Function1<G, B>() {
            @Override
            public B apply(G arg) {
                return f.apply(g.apply(arg));
            }
        };
    }

    public static <A, B, G> Function1<A, G> andThen(final Function1<A, B> f, final Function1<B, G> g) {
        return new Function1<A, G>() {
            @Override
            public G apply(A arg) {
                return g.apply(f.apply(arg));
            }
        };
    }

}

