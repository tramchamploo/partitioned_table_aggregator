package bean;

import aggregator.SubTableDataAggregatorFactory;
import function.FunctionReducer;
import function.big_decimal.BigDecimalFunctionReducer;
import function.doubled.DoubleFunctionReducer;
import function.integer.IntFunctionReducer;
import function.longl.LongFunctionReducer;
import function.map.MapFunctionAndColReducer;
import function.timestamp.TimestampFunctionReducer;
import grouper.MapGrouper;
import limiter.DefaultLimiter;
import sort_strategy.NormalSortStrategy;
import sort_strategy.SortStrategyFactory;
import com.google.common.base.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.profiler.Profiler;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.support.rowset.SqlRowSet;
import util.ProfilerFactory;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

/**
 * Created by guohang.bao on 15/10/28.
 */
public class SqlExecution {

    protected SubTableDataAggregatorFactory aggregatorFactory;

    protected JdbcTemplate jdbcTemplate;

    protected SortStrategyFactory sortStrategyFactory;

    private static SqlExecution instance;

    private Logger logger = LoggerFactory.getLogger(SqlExecution.class);

    private SqlExecution(JdbcTemplate jdbcTemplate,
                         SubTableDataAggregatorFactory aggregatorFactory,
                         SortStrategyFactory sortStrategyFactory) {
        this.jdbcTemplate = jdbcTemplate;
        this.aggregatorFactory = aggregatorFactory;
        this.sortStrategyFactory = sortStrategyFactory;
    }

    public static SqlExecution getInstance(JdbcTemplate jdbcTemplate,
                                           SubTableDataAggregatorFactory aggregatorFactory,
                                           SortStrategyFactory sortStrategyFactory) {
        if (instance == null) {
            instance = new SqlExecution(jdbcTemplate, aggregatorFactory, sortStrategyFactory);
        }

        return instance;
    }


    private static class PartitionedQueryException extends DataAccessException {

        public PartitionedQueryException(String msg, Throwable cause) {
            super(msg, cause);
        }
    }


    private void error(Exception e) {
        throw new PartitionedQueryException(e.getMessage(), e);
    }


    private List<Map<String, Object>> groupMapList(List<Map<String, Object>> unGrouped, Sql s) {
        return new MapGrouper().group(unGrouped, s);
    }


    private List<Map<String, Object>> sortMapList(List<Map<String, Object>> unSorted, Sql s) {
        NormalSortStrategy<Map<String, Object>> sortStrategy =
                (NormalSortStrategy<Map<String, Object>>) sortStrategyFactory.newInstance(s,
                        s.extractComparator(Map.class));

        if (sortStrategy == null) {
            return unSorted;
        } else {
            sortStrategy.submit(unSorted);
            return sortStrategy.result();
        }
    }


    private <T> List<T> limitList(List<T> unLimited, Sql s) {
        return new DefaultLimiter(s).limit(unLimited);
    }


    private List<Map<String, Object>> afterAggregated(List<Map<String, Object>> raw, Sql s) {
        return limitList(sortMapList(groupMapList(raw, s), s), s);
    }


    public List<Map<String, Object>> queryForList(String sql) throws DataAccessException {
        Optional<Sql> sqlOpt = Sql.fromString(sql);
        if (sqlOpt.isPresent()) {
            try {
                Sql s = sqlOpt.get();
                Comparator<Map> comparator = s.extractComparator(Map.class);
                List<Map<String, Object>> unGrouped = comparator == null ?
                        aggregatorFactory.newInstance(jdbcTemplate, s, Map.class).run() :
                        aggregatorFactory.newInstance(jdbcTemplate, s, comparator).run();

                return afterAggregated(unGrouped, s);
            } catch (Exception e) {
                error(e);
            }
        }
        return jdbcTemplate.queryForList(sql);
    }


    public List<Map<String, Object>> queryForList(String sql, Object... args) throws DataAccessException {
        Optional<Sql> sqlOpt = Sql.fromString(sql, args);
        if (sqlOpt.isPresent()) {
            try {
                Sql s = sqlOpt.get();
                Comparator<Map> comparator = s.extractComparator(Map.class);

                List<Map<String, Object>> unGrouped = comparator == null ?
                        aggregatorFactory.newInstance(jdbcTemplate, s, Map.class).run(s.trimArgs(args)) :
                        aggregatorFactory.newInstance(jdbcTemplate, s, comparator).run(s.trimArgs(args));

                return afterAggregated(unGrouped, s);
            } catch (Exception e) {
                error(e);
            }
        }
        return jdbcTemplate.queryForList(sql, args);
    }


    private <T> RowMapper<T> simpleRowMapper(Class<T> cls) {
        return new RowMapper<T>() {

            public T mapRow(ResultSet rs, int rowNum) throws SQLException {
                return (T) rs.getObject(1);
            }
        };
    }


    public <T> List<T> queryForList(String sql, Class<T> cls) throws DataAccessException {
        Optional<Sql> sqlOpt = Sql.fromString(sql);
        if (sqlOpt.isPresent()) {
            try {
                Sql s = sqlOpt.get();
                Comparator<T> comparator = s.extractComparator(cls);
                RowMapper<T> rowMapper = simpleRowMapper(cls);

                return limitList(comparator == null ?
                        aggregatorFactory.newInstance(jdbcTemplate, s, cls).run(rowMapper) :
                        aggregatorFactory.newInstance(jdbcTemplate, s, comparator).run(rowMapper), s);
            } catch (Exception e) {
                error(e);
            }
        }
        return jdbcTemplate.queryForList(sql, cls);
    }


    public <T> List<T> queryForList(String sql, Class<T> cls, Object... args) throws DataAccessException {
        Optional<Sql> sqlOpt = Sql.fromString(sql, args);
        if (sqlOpt.isPresent()) {
            try {
                Sql s = sqlOpt.get();
                Comparator<T> comparator = s.extractComparator(cls);
                RowMapper<T> rowMapper = simpleRowMapper(cls);

                return limitList(comparator == null ?
                        aggregatorFactory.newInstance(jdbcTemplate, s, cls).run(rowMapper, s.trimArgs(args)) :
                        aggregatorFactory.newInstance(jdbcTemplate, s, comparator).run(rowMapper, s.trimArgs(args)), s);
            } catch (Exception e) {
                error(e);
            }
        }
        return jdbcTemplate.queryForList(sql, cls, args);
    }


    public <T> List<T> query(String sql, RowMapper<T> rowMapper) throws DataAccessException {
        Profiler profiler = ProfilerFactory.createProfiler("JdbcTemplateProxy.query");

        profiler.start("parse sql");
        Optional<Sql> sqlOpt = Sql.fromString(sql);

        if (sqlOpt.isPresent()) {
            try {
                Sql s = sqlOpt.get();

                profiler.start("extract comparator");
                Comparator<SqlRowSet> comparator = s.extractComparator(SqlRowSet.class);

                profiler.start("executing query");
                List<T> ret = limitList(comparator == null ?
                        aggregatorFactory.newInstance(jdbcTemplate, s, SqlRowSet.class).runRowSet(rowMapper) :
                        aggregatorFactory.newInstance(jdbcTemplate, s, comparator).runRowSet(rowMapper), s);

                profiler.stop().print();
                return ret;
            } catch (Exception e) {
                error(e);
            }
        }
        return jdbcTemplate.query(sql, rowMapper);
    }


    public <T> List<T> query(String sql, RowMapper<T> rowMapper, Object... args) throws DataAccessException {
        Profiler profiler = ProfilerFactory.createProfiler("JdbcTemplateProxy.query");

        profiler.start("parse sql");
        Optional<Sql> sqlOpt = Sql.fromString(sql, args);

        if (sqlOpt.isPresent()) {
            try {
                Sql s = sqlOpt.get();

                profiler.start("extract comparator");
                Comparator<SqlRowSet> comparator = s.extractComparator(SqlRowSet.class);

                profiler.start("executing query");
                List<T> ret = limitList(comparator == null ?
                        aggregatorFactory.newInstance(jdbcTemplate, s, ResultSet.class)
                                .runRowSet(rowMapper, s.trimArgs(args)) :
                        aggregatorFactory.newInstance(jdbcTemplate, s, comparator)
                                .runRowSet(rowMapper, s.trimArgs(args)), s);

                profiler.stop().print();
                return ret;
            } catch (Exception e) {
                error(e);
            }
        }
        return jdbcTemplate.query(sql, rowMapper, args);
    }


    public <T> List<T> query(String sql, Object[] args, RowMapper<T> rowMapper) throws DataAccessException {
        return query(sql, rowMapper, args);
    }


    private FunctionReducer reducerByClass(Class<?> cls) {
        String clsName = cls.getName();
        if (clsName.equals("int") || clsName.equals("java.lang.Integer")) {
            return new IntFunctionReducer();
        } else if (clsName.equals("double") || clsName.equals("java.lang.Double")) {
            return new DoubleFunctionReducer();
        } else if (clsName.equals("long") || clsName.equals("java.lang.Long")) {
            return new LongFunctionReducer();
        } else if (clsName.equals("java.math.BigDecimal")) {
            return new BigDecimalFunctionReducer();
        } else if (clsName.equals("java.sql.Timestamp")) {
            return new TimestampFunctionReducer();
        }

        throw new UnsupportedOperationException("reduce not support cls: " + cls);
    }

    private FunctionReducer reducerByElement(List<?> elements) {
        if (elements != null && elements.size() > 0) {
            return reducerByClass(elements.get(0).getClass());
        }

        return null;
    }

    public <T> T queryForObject(String sql, Class<T> requiredType, Object... args) throws DataAccessException {
        Optional<Sql> sqlOpt = Sql.fromString(sql, args);

        if (sqlOpt.isPresent()) {

            Sql s = sqlOpt.get();

            try {
                if (s.canAddResult()) {
                    return aggregatorFactory.newInstance(jdbcTemplate, s, requiredType)
                            .runAddable(requiredType, s.trimArgs(args));

                } else if (s.haveFunctions()) {
                    List<T> raw = aggregatorFactory.newInstance(jdbcTemplate, s, requiredType)
                            .run(simpleRowMapper(requiredType), args);

                    return (T) reducerByElement(raw).reduced(raw, s);
                } else {
                    return aggregatorFactory.newInstance(jdbcTemplate, s, new Comparator<T>() {

                        public int compare(T o1, T o2) {
                            throw new UnsupportedOperationException();
                        }
                    }).runForOne(simpleRowMapper(requiredType), s.trimArgs(args));
                }
            } catch (Exception e) {
                error(e);
            }
        }

        return jdbcTemplate.queryForObject(sql, requiredType, args);
    }


    public <T> T queryForObject(String sql, Object[] args, Class<T> requiredType) throws DataAccessException {
        return queryForObject(sql, requiredType, args);
    }


    public <T> T queryForObject(String sql, Class<T> requiredType) throws DataAccessException {
        Optional<Sql> sqlOpt = Sql.fromString(sql);

        if (sqlOpt.isPresent()) {

            Sql s = sqlOpt.get();

            try {
                if (s.canAddResult()) {
                    return aggregatorFactory.newInstance(jdbcTemplate, s, requiredType)
                            .runAddable(requiredType);

                } else if (s.haveFunctions()) {
                    List<T> raw = aggregatorFactory.newInstance(jdbcTemplate, s, requiredType)
                            .run(simpleRowMapper(requiredType));

                    return (T) reducerByElement(raw).reduced(raw, s);
                } else {
                    return aggregatorFactory.newInstance(jdbcTemplate, s, new Comparator<T>() {

                        public int compare(T o1, T o2) {
                            throw new UnsupportedOperationException();
                        }
                    }).runForOne(simpleRowMapper(requiredType));
                }
            } catch (Exception e) {
                error(e);
            }
        }

        return jdbcTemplate.queryForObject(sql, requiredType);
    }


    public Map<String, Object> queryForMap(String sql, Object... args) throws DataAccessException {
        Profiler profiler = ProfilerFactory.createProfiler("JdbcTemplateProxy.queryForMap");

        profiler.start("parse sql");
        Optional<Sql> sqlOpt = Sql.fromString(sql, args);
        if (sqlOpt.isPresent()) {
            Sql s = sqlOpt.get();

            try {
                profiler.start("executing query");

                Map<String, Object> ret;

                if (s.haveFunctions()) {
                    List<Map<String, Object>> raw =
                            aggregatorFactory.newInstance(jdbcTemplate, s, Map.class).run(s.trimArgs(args));

                    ret = new MapFunctionAndColReducer().reduced(raw, s);
                } else {
                    ret = aggregatorFactory.newInstance(jdbcTemplate, s, Map.class)
                            .runForOne(s.trimArgs(args));
                }

                profiler.stop().print();
                return ret;
            } catch (Exception e) {
                error(e);
            }
        }

        return jdbcTemplate.queryForMap(sql, args);
    }


    public Map<String, Object> queryForMap(String sql) throws DataAccessException {
        Profiler profiler = ProfilerFactory.createProfiler("JdbcTemplateProxy.queryForMap");

        profiler.start("parse sql");
        Optional<Sql> sqlOpt = Sql.fromString(sql);
        if (sqlOpt.isPresent()) {
            Sql s = sqlOpt.get();

            try {
                profiler.start("executing query");

                Map<String, Object> ret;

                if (s.haveFunctions()) {
                    List<Map<String, Object>> raw =
                            aggregatorFactory.newInstance(jdbcTemplate, s, Map.class).run();

                    ret = new MapFunctionAndColReducer().reduced(raw, s);
                } else {
                    ret = aggregatorFactory.newInstance(jdbcTemplate, s, Map.class)
                            .runForOne();
                }

                profiler.stop().print();
                return ret;
            } catch (Exception e) {
                error(e);
            }
        }

        return jdbcTemplate.queryForMap(sql);
    }

}
