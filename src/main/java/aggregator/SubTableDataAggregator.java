package aggregator;

import bean.Sql;
import db_access_strategy.DBAccessStrategy;
import sort_strategy.NoSortStrategy;
import sort_strategy.SortStrategy;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.support.rowset.ResultSetWrappingSqlRowSet;

import java.util.Comparator;
import java.util.List;
import java.util.Map;

/**
 * Created by guohang.bao on 15-6-25.
 * Abstraction for aggregating partitioned data
 */
public abstract class SubTableDataAggregator<T> {

    private JdbcTemplate jdbcTemplate;

    protected Sql sql;

    protected Comparator<T> comparator;

    protected SubTableDataAggregator(JdbcTemplate jdbcTemplate, Sql sql,
                                     Comparator<T> comparator) {
        this.jdbcTemplate = jdbcTemplate;
        this.sql = sql;
        this.comparator = comparator;
    }

    protected abstract DBAccessStrategy<T> dbAccessStrategy();

    protected abstract SortStrategy<List<T>> listSortStrategy();

    protected abstract SortStrategy<List<Map<String, Object>>> mapListSortStrategy();

    protected abstract SortStrategy<ResultSetWrappingSqlRowSet> rowSetSortStrategy();

    private boolean notNeedSort() {
        return comparator == null || sql.cached().subSqls().length == 1;
    }

    private SortStrategy<List<T>> _listSortStrategy() {
        return notNeedSort() ? new NoSortStrategy<T>() : listSortStrategy();
    }

    private SortStrategy<List<Map<String, Object>>> _mapListSortStrategy() {
        return notNeedSort() ? new NoSortStrategy<Map<String, Object>>() : mapListSortStrategy();
    }

    public List<T> run(final RowMapper<T> rowMapper, Object... args) throws Exception {
        return dbAccessStrategy().load(jdbcTemplate, sql.cached().subSqls(), rowMapper, _listSortStrategy(), args);
    }

    @SuppressWarnings("unchecked")
    public List<Map<String, Object>> run(Object... args) throws Exception {
        return dbAccessStrategy().load(jdbcTemplate, sql.cached().subSqls(), _mapListSortStrategy(), args);
    }

    public <E> List<E> runRowSet(RowMapper<E> rowMapper, Object... args) throws Exception {
        return dbAccessStrategy().loadRowSet(jdbcTemplate, sql.cached().subSqls(), rowMapper, rowSetSortStrategy(), args);
    }

    public T runForOne(RowMapper<T> rowMapper, Object... args) throws Exception {
        return dbAccessStrategy().loadOne(jdbcTemplate, sql.cached().subSqls(), rowMapper, args);
    }

    public Map<String, Object> runForOne(Object... args) throws Exception {
        return dbAccessStrategy().loadOne(jdbcTemplate, sql.cached().subSqls(), args);
    }

    public T runAddable(Class<T> cls, Object... args) throws Exception {
        return dbAccessStrategy().loadAddable(jdbcTemplate, cls, sql.cached().subSqls(), args);
    }

    public int update(Object... args) throws Exception {
        return dbAccessStrategy().update(jdbcTemplate, sql.cached().subSqls(), args);
    }
}
