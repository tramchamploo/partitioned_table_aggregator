package partition;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

/**
 * Created by guohang.bao on 15-6-25.
 */
public class NoSortStrategy<T, E extends Comparable<E>> implements SortStrategy<T, E> {

    private List<T> result1;

    private List<E> result2;

    @Override
    public void submit(List<T> result) {
        if (result1 == null) {
            result1 = Collections.synchronizedList(new LinkedList<T>());
        }
        result1.addAll(result);
    }

    @Override
    public void submitComparable(List<E> result) {
        if (result2 == null) {
            result2 = Collections.synchronizedList(new LinkedList<E>());
        }
        result2.addAll(result);
    }

    @Override
    public List<T> result() {
        return result1;
    }

    @Override
    public List<E> selfComparableResult() {
        return result2;
    }
}
