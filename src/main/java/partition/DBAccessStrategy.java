package partition;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;

import java.util.List;
import java.util.Map;

/**
 * Created by guohang.bao on 15-6-25.
 */
public interface DBAccessStrategy<T, E extends Comparable<E>> {

    List<T> load(JdbcTemplate jdbcTemplate, String[] sqls, RowMapper<T> rowMapper, SortStrategy<T, E> sortStrategy, Object... args) throws Exception;

    T loadOne(JdbcTemplate jdbcTemplate, String[] sqls, RowMapper<T> rowMapper, Object... args) throws Exception;

    List<Map<String, Object>> load(JdbcTemplate jdbcTemplate, String[] sqls, SortStrategy<Map<String, Object>, E> sortStrategy, Object... args) throws Exception;

    Long loadCount(JdbcTemplate jdbcTemplate, String[] sqls, Object... args) throws Exception;

    List<E> loadSelfComparable(JdbcTemplate jdbcTemplate, String[] sqls, RowMapper<E> rowMapper, SortStrategy<T, E> sortStrategy, Object... args) throws Exception;

}
