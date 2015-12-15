package sort_strategy;

/**
 * Created by guohang.bao on 15-6-25.
 * abstraction for sort strategy
 */
public interface SortStrategy<T> {

    /**
     * when a sub query is done, use this to submit
     */
    void submit(T result);

    /**
     * return sorted result
     */
    T result();

    SortStrategy<T> newInstance();

    /**
     * trim result to expected size
     */
    void trim();
}
