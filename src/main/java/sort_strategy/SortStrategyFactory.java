package sort_strategy;

import bean.Sql;

import java.util.Comparator;

/**
 * Created by guohang.bao on 15-6-25.
 * abstraction for sort strategy
 */
public interface SortStrategyFactory<S extends SortStrategy> {

    <T> S newInstance(Sql sql, Comparator<T> comparator);

}
