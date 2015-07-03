package partition;

import java.util.List;

/**
 * Created by guohang.bao on 15-6-25.
 */
public interface SortStrategy<T, E extends Comparable<E>> {

    void submit(List<T> result);

    void submitComparable(List<E> result);

    List<T> result();

    List<E> selfComparableResult();
}
