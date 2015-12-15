package sort_strategy;

import bean.Sql;

import java.util.Comparator;

/**
 * Created by guohang.bao on 15-6-25.
 * abstraction for sort strategy
 */
public class NormalSortStrategyFactory implements SortStrategyFactory<NormalSortStrategy> {

    public <T> NormalSortStrategy newInstance(Sql sql, Comparator<T> comparator) {
        if (comparator == null) return null;
        return new NormalSortStrategy<T>(sql, comparator);
    }
}
