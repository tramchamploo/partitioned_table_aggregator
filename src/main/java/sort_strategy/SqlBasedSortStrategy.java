package sort_strategy;

import bean.Limit;
import bean.Sql;

/**
 * Created by guohang.bao on 15-6-25.
 * If sort strategy need to know about sql, extends this class
 */
public abstract class SqlBasedSortStrategy<T> implements SortStrategy<T> {

    protected Sql sql;

    protected Limit limit;

    protected SqlBasedSortStrategy(Sql sql) {
        this.sql = sql;
        this.limit = sql.limit();
    }

}
