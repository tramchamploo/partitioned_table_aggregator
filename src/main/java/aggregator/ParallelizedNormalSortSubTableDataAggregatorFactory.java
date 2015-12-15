package aggregator;

import bean.Sql;
import org.springframework.jdbc.core.JdbcTemplate;

import java.util.Comparator;

/**
 * Created by guohang.bao on 15-10-14.
 */
public class ParallelizedNormalSortSubTableDataAggregatorFactory implements SubTableDataAggregatorFactory {

    private int parallelism;

    public ParallelizedNormalSortSubTableDataAggregatorFactory(int parallelism) {
        this.parallelism = parallelism;
    }

    public ParallelizedNormalSortSubTableDataAggregatorFactory() {
    }

    public <T> SubTableDataAggregator<T> newInstance(JdbcTemplate jdbcTemplate, Sql sql, Comparator<T> comparator) {
        return ParallelizedNormalSortSubTableDataAggregator.of(jdbcTemplate, sql, comparator, parallelism);
    }

    public <T> SubTableDataAggregator<T> newInstance(JdbcTemplate jdbcTemplate, Sql sql, Class<T> clz) {
        return ParallelizedNormalSortSubTableDataAggregator.of(jdbcTemplate, sql, clz, parallelism);
    }
}
