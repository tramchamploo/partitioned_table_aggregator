package sort_strategy;

import bean.Sql;

import java.util.List;

/**
 * Created by guohang.bao on 15-8-27.
 */
public abstract class ListSortStrategy<E> extends SqlBasedSortStrategy<List<E>> {

    protected ListSortStrategy(Sql sql) {
        super(sql);
    }

}
