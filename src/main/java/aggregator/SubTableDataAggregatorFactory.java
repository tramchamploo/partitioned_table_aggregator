package aggregator;

import bean.Sql;
import org.springframework.jdbc.core.JdbcTemplate;

import java.util.Comparator;

/**
 * Created by guohang.bao on 15-10-14.
 */
public interface SubTableDataAggregatorFactory {

    <T> SubTableDataAggregator<T> newInstance(JdbcTemplate jdbcTemplate,
                                              Sql sql,
                                              Comparator<T> comparator);

    <T> SubTableDataAggregator<T> newInstance(JdbcTemplate jdbcTemplate,
                                              Sql sql, Class<T> clz);


}
