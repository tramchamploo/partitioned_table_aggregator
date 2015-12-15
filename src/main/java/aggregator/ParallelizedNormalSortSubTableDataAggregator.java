package aggregator;

import bean.Sql;
import db_access_strategy.DBAccessStrategy;
import db_access_strategy.ParallelizedDBAccessStrategy;
import sort_strategy.NormalSortStrategy;
import sort_strategy.RowSetSortStrategy;
import sort_strategy.SortStrategy;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.support.rowset.ResultSetWrappingSqlRowSet;
import org.springframework.jdbc.support.rowset.SqlRowSet;

import java.util.Comparator;
import java.util.List;
import java.util.Map;

/**
 * Created by guohang.bao on 15-6-25.
 */
public final class ParallelizedNormalSortSubTableDataAggregator<T>
        extends SubTableDataAggregator<T> {

    private int parallelism;

    private ParallelizedNormalSortSubTableDataAggregator(JdbcTemplate jdbcTemplate,
                                                         Sql sql,
                                                         Comparator<T> comparator,
                                                         int parallelism) {
        super(jdbcTemplate, sql, comparator);
        this.parallelism = parallelism;
    }

    @Override
    protected DBAccessStrategy<T> dbAccessStrategy() {
        return parallelism > 0 ?
                new ParallelizedDBAccessStrategy<T>(sql, parallelism) :
                new ParallelizedDBAccessStrategy<T>(sql);
    }

    @Override
    protected SortStrategy<List<T>> listSortStrategy() {
        return new NormalSortStrategy<T>(sql, comparator);
    }

    @Override
    @SuppressWarnings("unchecked")
    protected SortStrategy<List<Map<String, Object>>> mapListSortStrategy() {
        return new NormalSortStrategy<Map<String, Object>>(sql, (Comparator<Map<String, Object>>) comparator);
    }

    @Override
    @SuppressWarnings("unchecked")
    protected SortStrategy<ResultSetWrappingSqlRowSet> rowSetSortStrategy() {
        return new RowSetSortStrategy(sql, (Comparator<SqlRowSet>) comparator);
    }

    public static <T> ParallelizedNormalSortSubTableDataAggregator<T> of(JdbcTemplate jdbcTemplate,
                                                                         Sql sql,
                                                                         Comparator<T> comparator,
                                                                         int parallelism) {
        return new ParallelizedNormalSortSubTableDataAggregator<T>(jdbcTemplate, sql, comparator, parallelism);
    }

    public static <T> ParallelizedNormalSortSubTableDataAggregator<T> of(JdbcTemplate jdbcTemplate,
                                                                         Sql sql,
                                                                         Class<T> clz,
                                                                         int parallelism) {
        return new ParallelizedNormalSortSubTableDataAggregator<T>(jdbcTemplate, sql, null, parallelism);
    }

}
