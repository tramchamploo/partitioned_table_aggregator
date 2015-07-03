package partition;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;

import java.util.List;
import java.util.Map;

/**
 * Created by guohang.bao on 15-6-25.
 */
public abstract class SubTableDataAggregator<T, E extends Comparable<E>> {

    private JdbcTemplate jdbcTemplate;

    protected Sql sql;

    protected SubTableDataAggregator(JdbcTemplate jdbcTemplate, Sql sql) {
        this.jdbcTemplate = jdbcTemplate;
        this.sql = sql;
    }

    public abstract List<T> run(final RowMapper<T> rowMapper, Object... args) throws Exception;

    protected List<T> run(DBAccessStrategy<T, E> dbAccessStrategy, SortStrategy<T, E> sortStrategy,
                          final RowMapper<T> rowMapper, Object... args) throws Exception {
        return dbAccessStrategy.load(jdbcTemplate, sql.subSqls(), rowMapper, sortStrategy, args);
    }

    public abstract List<Map<String, Object>> run(Object... args) throws Exception;

    @SuppressWarnings("unchecked")
    protected List<Map<String, Object>> run(DBAccessStrategy<T, E> dbAccessStrategy, SortStrategy<T, E> sortStrategy, Object... args) throws Exception {
        return dbAccessStrategy.load(jdbcTemplate, sql.subSqls(), (SortStrategy<Map<String, Object>, E>) sortStrategy, args);
    }

    public abstract T runForOne(RowMapper<T> rowMapper, Object... args) throws Exception;

    protected T runForOne(DBAccessStrategy<T, E> dbAccessStrategy, RowMapper<T> rowMapper, Object... args) throws Exception {
        return dbAccessStrategy.loadOne(jdbcTemplate, sql.subSqls(), rowMapper, args);
    }

    protected Long runCount(DBAccessStrategy<T, E> dbAccessStrategy, Object... args) throws Exception {
        return dbAccessStrategy.loadCount(jdbcTemplate, sql.subSqls(), args);
    }

    public abstract Long runCount(Object... args) throws Exception;

    public abstract List<E> runForComparable(final RowMapper<E> rowMapper, Object... args) throws Exception;

    protected List<E> runForComparable(DBAccessStrategy<T, E> dbAccessStrategy, SortStrategy<T, E> sortStrategy,
                                       final RowMapper<E> rowMapper, Object... args) throws Exception {
        return dbAccessStrategy.loadSelfComparable(jdbcTemplate, sql.subSqls(), rowMapper, sortStrategy, args);
    }
}
