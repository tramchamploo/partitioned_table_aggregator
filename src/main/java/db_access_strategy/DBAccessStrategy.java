package db_access_strategy;

import sort_strategy.SortStrategy;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.support.rowset.ResultSetWrappingSqlRowSet;

import java.util.List;
import java.util.Map;

/**
 * Created by guohang.bao on 15-6-25.
 */
public interface DBAccessStrategy<T> {

    T loadOne(JdbcTemplate jdbcTemplate, String[] sqls, RowMapper<T> rowMapper, Object... args) throws Exception;


    Map<String, Object> loadOne(JdbcTemplate jdbcTemplate, String[] sqls, Object[] args) throws Exception;


    List<T> load(JdbcTemplate jdbcTemplate, String[] sqls, RowMapper<T> rowMapper, SortStrategy<List<T>> sortStrategy,
                 Object... args) throws Exception;


    List<Map<String, Object>> load(JdbcTemplate jdbcTemplate, String[] sqls,
                                   SortStrategy<List<Map<String, Object>>> listSortStrategy,
                                   Object... args) throws Exception;


    <E> List<E> loadRowSet(JdbcTemplate jdbcTemplate, String[] sqls, RowMapper<E> rowMapper,
                           SortStrategy<ResultSetWrappingSqlRowSet> sortStrategy, Object... args) throws Exception;


    T loadAddable(JdbcTemplate jdbcTemplate, Class<T> cls, String[] sqls, Object... args) throws Exception;


    int update(JdbcTemplate jdbcTemplate, String[] sqls, Object[] args);

}
