package util;

import aggregator.ParallelizedNormalSortSubTableDataAggregatorFactory;
import aggregator.SubTableDataAggregatorFactory;
import bean.SqlExecution;
import sort_strategy.NormalSortStrategyFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;

import java.util.List;
import java.util.Map;

/**
 * Created by guohang.bao on 15-8-10.
 */
public class JdbcTemplateProxy extends JdbcTemplate {

    private SqlExecution execution;

    public JdbcTemplateProxy(JdbcTemplate proxied, SubTableDataAggregatorFactory aggregatorFactory) {
        super(proxied.getDataSource());
        this.execution = SqlExecution.getInstance(proxied, aggregatorFactory, new NormalSortStrategyFactory());
    }

    public JdbcTemplateProxy(JdbcTemplate proxied) {
        this(proxied, new ParallelizedNormalSortSubTableDataAggregatorFactory());
    }

    @Override
    public List<Map<String, Object>> queryForList(String sql) throws DataAccessException {
        return execution.queryForList(sql);
    }

    @Override
    public List<Map<String, Object>> queryForList(String sql, Object... args) throws DataAccessException {
        return execution.queryForList(sql, args);
    }

    @Override
    public <T> List<T> queryForList(String sql, Class<T> cls) throws DataAccessException {
        return execution.queryForList(sql, cls);
    }

    @Override
    public <T> List<T> queryForList(String sql, Class<T> cls, Object... args) throws DataAccessException {
        return execution.queryForList(sql, cls, args);
    }

    @Override
    public <T> List<T> query(String sql, RowMapper<T> rowMapper) throws DataAccessException {
        return execution.query(sql, rowMapper);
    }

    @Override
    public <T> List<T> query(String sql, RowMapper<T> rowMapper, Object... args) throws DataAccessException {
        return execution.query(sql, rowMapper, args);
    }

    @Override
    public <T> List<T> query(String sql, Object[] args, RowMapper<T> rowMapper) throws DataAccessException {
        return execution.query(sql, args, rowMapper);
    }

    @Override
    public <T> T queryForObject(String sql, Class<T> requiredType, Object... args) throws DataAccessException {
        return execution.queryForObject(sql, requiredType, args);
    }

    @Override
    public <T> T queryForObject(String sql, Object[] args, Class<T> requiredType) throws DataAccessException {
        return execution.queryForObject(sql, args, requiredType);
    }

    @Override
    public <T> T queryForObject(String sql, Class<T> requiredType) throws DataAccessException {
        return execution.queryForObject(sql, requiredType);
    }

    @Override
    public Map<String, Object> queryForMap(String sql, Object... args) throws DataAccessException {
        return execution.queryForMap(sql, args);
    }

    @Override
    public Map<String, Object> queryForMap(String sql) throws DataAccessException {
        return execution.queryForMap(sql);
    }

}
