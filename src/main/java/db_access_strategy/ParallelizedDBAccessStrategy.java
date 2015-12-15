package db_access_strategy;

import bean.Sql;
import sort_strategy.SortStrategy;
import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.profiler.Profiler;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.support.rowset.ResultSetWrappingSqlRowSet;
import util.BaseUtils;
import util.ProfilerFactory;
import util.PublicExecutorService;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.ResultSet;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.RecursiveTask;

/**
 * Created by guohang.bao on 15-6-25.
 */
public class ParallelizedDBAccessStrategy<T> implements DBAccessStrategy<T> {

    private Logger logger = LoggerFactory.getLogger(ParallelizedDBAccessStrategy.class);

    private Sql sql;

    private int parallelism;

    private int queriesPerTask;

    /**
     * Thread pool for merge
     */
    private static ForkJoinPool forkJoinPool = new ForkJoinPool(50);

    private static PublicExecutorService executorService = new PublicExecutorService();


    public ParallelizedDBAccessStrategy(Sql sql, int parallelism) {
        this.sql = sql;
        this.parallelism = parallelism;
        this.queriesPerTask = sql.subSqls().length / parallelism;
    }


    public ParallelizedDBAccessStrategy(Sql sql) {
        this(sql, sql.subSqls().length);
    }


    private void logQuery(String sql, Object result) {
        logger.debug("subsql sql execution: {}, {}", sql, result);
    }


    /**
     * base task for merging sub queries
     */
    private static abstract class ProcessingQueries<T, Self extends ProcessingQueries<T, Self>> extends RecursiveTask<T> {

        protected Logger logger = LoggerFactory.getLogger(ProcessingQueries.class);

        protected String[] sqls;

        protected JdbcTemplate jdbcTemplate;

        protected Object[] args;

        protected int queriesPerTask;


        protected ProcessingQueries(String[] sqls,
                                    JdbcTemplate jdbcTemplate,
                                    Object[] args,
                                    int queriesPerTask) {
            this.sqls = sqls;
            this.jdbcTemplate = jdbcTemplate;
            this.args = args;
            this.queriesPerTask = queriesPerTask;
        }


        /**
         * each thread has one query by default
         */
        protected boolean needNotFork() {
            return sqls.length <= queriesPerTask;
        }


        /**
         * actually do the query
         */
        protected abstract T doQuery(String sql);


        /**
         * merge results from doQuery()
         */
        protected T query() {
            T firstResult = doQuery(sqls[0]);

            if (queriesPerTask == 1) {
                return firstResult;
            } else {
                T accumulated = firstResult;

                for (int i = 1; i < sqls.length; i++) {
                    T result = doQuery(sqls[i]);
                    accumulated = reducedWithLog(accumulated, result);
                }

                return accumulated;
            }
        }


        private T reducedWithLog(T t1, T t2) {
            logger.debug("reducing {}, {}", t1, t2);

            T ret = reducer(t1, t2);
            logger.debug("result: {}", ret);

            return ret;
        }


        /**
         * sub query result reducer
         */
        protected abstract T reducer(T t1, T t2);


        /**
         * split to sub tasks
         */
        protected abstract Self[] split();


        /**
         * calculate the result recursively
         */
        @Override
        protected T compute() {
            Profiler profiler = ProfilerFactory.createProfiler("ProcessingQueries.compute");
            profiler.setLogger(logger);

            if (needNotFork()) {
                profiler.start("doQuery");
                T query = query();

                profiler.setLogger(logger);
                profiler.stop().log();
                return query;
            }

            Self[] queries = split();

            T result = null;
            profiler.start("fork and join");

            for (int i = 0; i < queries.length - 1; i++) {
                queries[i + 1].fork();

                if (i == 0) result = reducedWithLog(queries[i].compute(), queries[i + 1].join());
                else result = reducedWithLog(result, queries[i + 1].join());
            }

            profiler.stop().log();
            return result;
        }
    }

    /**
     * used for querying for list from sub tables.
     * basically for List<Map<String, Object>>
     */
    private static class ProcessingListQueries<T>
            extends ProcessingQueries<List<T>, ProcessingListQueries<T>> {

        protected SortStrategy<List<T>> sortStrategy;

        protected ProcessingListQueries(String[] sqls,
                                        JdbcTemplate jdbcTemplate,
                                        Object[] args,
                                        SortStrategy<List<T>> sortStrategy,
                                        int queriesPerTask) {
            super(sqls, jdbcTemplate, args, queriesPerTask);
            this.sortStrategy = sortStrategy.newInstance();
        }


        /**
         * no RowMapper provided, so a type cast is ok.
         */
        @Override
        @SuppressWarnings("unchecked")
        protected List<T> doQuery(String sql) {
            return (List<T>) (BaseUtils.noArgs(args) ? jdbcTemplate.queryForList(sql) :
                    jdbcTemplate.queryForList(sql, args));
        }


        /**
         * use sort strategy to reduce
         */
        @Override
        protected List<T> reducer(List<T> t1, List<T> t2) {
            sortStrategy.submit(t1);
            sortStrategy.submit(t2);
            sortStrategy.trim();
            return sortStrategy.result();
        }


        @Override
        protected ProcessingListQueries<T>[] split() {
            if (sqls.length > 1) {
                int mid = sqls.length / 2;

                ProcessingListQueries<T> left =
                        new ProcessingListQueries<T>(Arrays.copyOfRange(sqls, 0, mid),
                                jdbcTemplate, args, sortStrategy, queriesPerTask);
                ProcessingListQueries<T> right =
                        new ProcessingListQueries<T>(Arrays.copyOfRange(sqls, mid, sqls.length),
                                jdbcTemplate, args, sortStrategy, queriesPerTask);

                return (ProcessingListQueries<T>[]) new ProcessingListQueries[]{left, right};
            }

            return (ProcessingListQueries<T>[]) new ProcessingListQueries[]{this};
        }
    }


    private static class ProcessingListQueriesWithRowMapper<T> extends ProcessingListQueries<T> {

        private RowMapper<T> rowMapper;

        protected ProcessingListQueriesWithRowMapper(String[] sqls,
                                                     JdbcTemplate jdbcTemplate,
                                                     Object[] args,
                                                     SortStrategy<List<T>> sortStrategy,
                                                     RowMapper<T> rowMapper,
                                                     int queriesPerTask) {
            super(sqls, jdbcTemplate, args, sortStrategy, queriesPerTask);
            this.rowMapper = rowMapper;
        }

        /**
         * RowMapper provided.
         */
        @Override
        protected List<T> doQuery(String sql) {
            return BaseUtils.noArgs(args) ? jdbcTemplate.query(sql, rowMapper) :
                    jdbcTemplate.query(sql, rowMapper, args);
        }

        @Override
        protected ProcessingListQueriesWithRowMapper<T>[] split() {
            if (sqls.length > 1) {
                int mid = sqls.length / 2;

                ProcessingListQueriesWithRowMapper<T> left =
                        new ProcessingListQueriesWithRowMapper<T>(Arrays.copyOfRange(sqls, 0, mid),
                                jdbcTemplate, args, sortStrategy, rowMapper, queriesPerTask);
                ProcessingListQueriesWithRowMapper<T> right =
                        new ProcessingListQueriesWithRowMapper<T>(Arrays.copyOfRange(sqls, mid, sqls.length),
                                jdbcTemplate, args, sortStrategy, rowMapper, queriesPerTask);

                return (ProcessingListQueriesWithRowMapper<T>[]) new ProcessingListQueriesWithRowMapper[]{left, right};
            }

            return (ProcessingListQueriesWithRowMapper<T>[]) new ProcessingListQueriesWithRowMapper[]{this};
        }

    }

    private static abstract class ProcessingAddableQueries<N extends Number>
            extends ProcessingQueries<N, ProcessingAddableQueries<N>> {


        protected ProcessingAddableQueries(String[] sqls,
                                           JdbcTemplate jdbcTemplate,
                                           Object[] args,
                                           int queriesPerTask) {
            super(sqls, jdbcTemplate, args, queriesPerTask);
        }

    }

    private static class ProcessingIntQueries extends ProcessingAddableQueries<Integer> {
        protected ProcessingIntQueries(String[] sqls, JdbcTemplate jdbcTemplate, Object[] args, int queriesPerTask) {
            super(sqls, jdbcTemplate, args, queriesPerTask);
        }

        @Override
        protected Integer doQuery(String sql) {
            return BaseUtils.noArgs(args) ? jdbcTemplate.queryForObject(sql, Integer.class) :
                    jdbcTemplate.queryForObject(sql, Integer.class, args);
        }

        @Override
        protected Integer reducer(Integer t1, Integer t2) {
            return t1 + t2;
        }

        @Override
        protected ProcessingAddableQueries<Integer>[] split() {
            if (sqls.length > 1) {
                int mid = sqls.length / 2;

                ProcessingAddableQueries<Integer> left =
                        new ProcessingIntQueries(Arrays.copyOfRange(sqls, 0, mid),
                                jdbcTemplate, args, queriesPerTask);
                ProcessingAddableQueries<Integer> right =
                        new ProcessingIntQueries(Arrays.copyOfRange(sqls, mid, sqls.length),
                                jdbcTemplate, args, queriesPerTask);

                return new ProcessingAddableQueries[]{left, right};
            }

            return new ProcessingAddableQueries[]{this};
        }
    }

    private static class ProcessingLongQueries extends ProcessingAddableQueries<Long> {

        protected ProcessingLongQueries(String[] sqls, JdbcTemplate jdbcTemplate, Object[] args, int queriesPerTask) {
            super(sqls, jdbcTemplate, args, queriesPerTask);
        }

        @Override
        protected Long doQuery(String sql) {
            return BaseUtils.noArgs(args) ? jdbcTemplate.queryForObject(sql, Long.class) :
                    jdbcTemplate.queryForObject(sql, Long.class, args);
        }

        @Override
        protected Long reducer(Long t1, Long t2) {
            return t1 + t2;
        }

        @Override
        protected ProcessingAddableQueries<Long>[] split() {
            if (sqls.length > 1) {
                int mid = sqls.length / 2;

                ProcessingAddableQueries<Long> left =
                        new ProcessingLongQueries(Arrays.copyOfRange(sqls, 0, mid),
                                jdbcTemplate, args, queriesPerTask);
                ProcessingAddableQueries<Long> right =
                        new ProcessingLongQueries(Arrays.copyOfRange(sqls, mid, sqls.length),
                                jdbcTemplate, args, queriesPerTask);

                return new ProcessingAddableQueries[]{left, right};
            }

            return new ProcessingAddableQueries[]{this};
        }
    }

    private static class ProcessingFloatQueries extends ProcessingAddableQueries<Float> {

        protected ProcessingFloatQueries(String[] sqls, JdbcTemplate jdbcTemplate, Object[] args, int queriesPerTask) {
            super(sqls, jdbcTemplate, args, queriesPerTask);
        }

        @Override
        protected Float doQuery(String sql) {
            return BaseUtils.noArgs(args) ? jdbcTemplate.queryForObject(sql, Float.class) :
                    jdbcTemplate.queryForObject(sql, Float.class, args);
        }

        @Override
        protected Float reducer(Float t1, Float t2) {
            return t1 + t2;
        }

        @Override
        protected ProcessingAddableQueries<Float>[] split() {
            if (sqls.length > 1) {
                int mid = sqls.length / 2;

                ProcessingAddableQueries<Float> left =
                        new ProcessingFloatQueries(Arrays.copyOfRange(sqls, 0, mid),
                                jdbcTemplate, args, queriesPerTask);
                ProcessingAddableQueries<Float> right =
                        new ProcessingFloatQueries(Arrays.copyOfRange(sqls, mid, sqls.length),
                                jdbcTemplate, args, queriesPerTask);

                return new ProcessingAddableQueries[]{left, right};
            }

            return new ProcessingAddableQueries[]{this};
        }
    }

    private static class ProcessingDoubleQueries extends ProcessingAddableQueries<Double> {

        protected ProcessingDoubleQueries(String[] sqls, JdbcTemplate jdbcTemplate, Object[] args, int queriesPerTask) {
            super(sqls, jdbcTemplate, args, queriesPerTask);
        }

        @Override
        protected Double doQuery(String sql) {
            return BaseUtils.noArgs(args) ? jdbcTemplate.queryForObject(sql, Double.class) :
                    jdbcTemplate.queryForObject(sql, Double.class, args);
        }

        @Override
        protected Double reducer(Double t1, Double t2) {
            return t1 + t2;
        }

        @Override
        protected ProcessingAddableQueries<Double>[] split() {
            if (sqls.length > 1) {
                int mid = sqls.length / 2;

                ProcessingAddableQueries<Double> left =
                        new ProcessingDoubleQueries(Arrays.copyOfRange(sqls, 0, mid),
                                jdbcTemplate, args, queriesPerTask);
                ProcessingAddableQueries<Double> right =
                        new ProcessingDoubleQueries(Arrays.copyOfRange(sqls, mid, sqls.length),
                                jdbcTemplate, args, queriesPerTask);

                return new ProcessingAddableQueries[]{left, right};
            }

            return new ProcessingAddableQueries[]{this};
        }
    }

    private static class ProcessingBigIntegerQueries extends ProcessingAddableQueries<BigInteger> {

        protected ProcessingBigIntegerQueries(String[] sqls, JdbcTemplate jdbcTemplate, Object[] args, int queriesPerTask) {
            super(sqls, jdbcTemplate, args, queriesPerTask);
        }

        @Override
        protected BigInteger doQuery(String sql) {
            return BaseUtils.noArgs(args) ? jdbcTemplate.queryForObject(sql, BigInteger.class) :
                    jdbcTemplate.queryForObject(sql, BigInteger.class, args);
        }

        @Override
        protected BigInteger reducer(BigInteger t1, BigInteger t2) {
            return t1 == null ? t2 : t2 == null ? t1 : t1.add(t2);
        }

        @Override
        protected ProcessingAddableQueries<BigInteger>[] split() {
            if (sqls.length > 1) {
                int mid = sqls.length / 2;

                ProcessingAddableQueries<BigInteger> left =
                        new ProcessingBigIntegerQueries(Arrays.copyOfRange(sqls, 0, mid),
                                jdbcTemplate, args, queriesPerTask);
                ProcessingAddableQueries<BigInteger> right =
                        new ProcessingBigIntegerQueries(Arrays.copyOfRange(sqls, mid, sqls.length),
                                jdbcTemplate, args, queriesPerTask);

                return new ProcessingAddableQueries[]{left, right};
            }

            return new ProcessingAddableQueries[]{this};
        }
    }

    private static class ProcessingBigDecimalQueries extends ProcessingAddableQueries<BigDecimal> {

        protected ProcessingBigDecimalQueries(String[] sqls, JdbcTemplate jdbcTemplate, Object[] args, int queriesPerTask) {
            super(sqls, jdbcTemplate, args, queriesPerTask);
        }

        @Override
        protected BigDecimal doQuery(String sql) {
            return BaseUtils.noArgs(args) ? jdbcTemplate.queryForObject(sql, BigDecimal.class) :
                    jdbcTemplate.queryForObject(sql, BigDecimal.class, args);
        }

        @Override
        protected BigDecimal reducer(BigDecimal t1, BigDecimal t2) {
            return t1 == null ? t2 : t2 == null ? t1 : t1.add(t2);
        }

        @Override
        protected ProcessingAddableQueries<BigDecimal>[] split() {
            if (sqls.length > 1) {
                int mid = sqls.length / 2;

                ProcessingAddableQueries<BigDecimal> left =
                        new ProcessingBigDecimalQueries(Arrays.copyOfRange(sqls, 0, mid),
                                jdbcTemplate, args, queriesPerTask);
                ProcessingAddableQueries<BigDecimal> right =
                        new ProcessingBigDecimalQueries(Arrays.copyOfRange(sqls, mid, sqls.length),
                                jdbcTemplate, args, queriesPerTask);

                return new ProcessingAddableQueries[]{left, right};
            }

            return new ProcessingAddableQueries[]{this};
        }
    }

    private static class ProcessingUpdate
            extends ProcessingQueries<Integer, ProcessingUpdate> {


        protected ProcessingUpdate(String[] sqls,
                                   JdbcTemplate jdbcTemplate,
                                   Object[] args,
                                   int queriesPerTask) {
            super(sqls, jdbcTemplate, args, queriesPerTask);
        }

        @Override
        protected Integer doQuery(String sql) {
            return BaseUtils.noArgs(args) ? jdbcTemplate.update(sql) :
                    jdbcTemplate.update(sql, args);
        }

        /**
         * add the affected row numbers
         */
        @Override
        protected Integer reducer(Integer t1, Integer t2) {
            return t1 + t2;
        }

        @Override
        protected ProcessingUpdate[] split() {
            if (sqls.length > 1) {
                int mid = sqls.length / 2;

                ProcessingUpdate left =
                        new ProcessingUpdate(Arrays.copyOfRange(sqls, 0, mid),
                                jdbcTemplate, args, queriesPerTask);
                ProcessingUpdate right =
                        new ProcessingUpdate(Arrays.copyOfRange(sqls, mid, sqls.length),
                                jdbcTemplate, args, queriesPerTask);

                return new ProcessingUpdate[]{left, right};
            }

            return new ProcessingUpdate[]{this};
        }
    }

    /**
     * task that process rowset query
     */
    private static class ProcessingRowSetQueries
            extends ProcessingQueries<ResultSetWrappingSqlRowSet, ProcessingRowSetQueries> {

        private SortStrategy<ResultSetWrappingSqlRowSet> sortStrategy;

        protected ProcessingRowSetQueries(String[] sqls,
                                          JdbcTemplate jdbcTemplate,
                                          Object[] args,
                                          SortStrategy<ResultSetWrappingSqlRowSet> sortStrategy,
                                          int queriesPerTask) {
            super(sqls, jdbcTemplate, args, queriesPerTask);
            this.sortStrategy = sortStrategy.newInstance();
        }

        @Override
        protected ResultSetWrappingSqlRowSet doQuery(String sql) {
            return (ResultSetWrappingSqlRowSet) (BaseUtils.noArgs(args) ? jdbcTemplate.queryForRowSet(sql) :
                    jdbcTemplate.queryForRowSet(sql, args));
        }

        @Override
        protected ResultSetWrappingSqlRowSet reducer(ResultSetWrappingSqlRowSet t1, ResultSetWrappingSqlRowSet t2) {
            sortStrategy.submit(t1);
            sortStrategy.submit(t2);
            return sortStrategy.result();
        }

        @Override
        protected ProcessingRowSetQueries[] split() {
            if (sqls.length > 1) {
                int mid = sqls.length / 2;

                ProcessingRowSetQueries left =
                        new ProcessingRowSetQueries(Arrays.copyOfRange(sqls, 0, mid),
                                jdbcTemplate, args, sortStrategy, queriesPerTask);
                ProcessingRowSetQueries right =
                        new ProcessingRowSetQueries(Arrays.copyOfRange(sqls, mid, sqls.length),
                                jdbcTemplate, args, sortStrategy, queriesPerTask);

                return new ProcessingRowSetQueries[]{left, right};
            }

            return new ProcessingRowSetQueries[]{this};
        }
    }


    /**
     * load list result
     */
    public List<T> load(final JdbcTemplate jdbcTemplate, final String[] sqls, final RowMapper<T> rowMapper,
                        final SortStrategy<List<T>> sortStrategy, final Object... args) throws Exception {
        return forkJoinPool.invoke(
                new ProcessingListQueriesWithRowMapper<T>(sqls, jdbcTemplate, args,
                        sortStrategy, rowMapper, queriesPerTask));
    }


    /**
     * send threads to get the only result, who gets first gets the result
     */
    public T loadOne(final JdbcTemplate jdbcTemplate, final String[] sqls, final RowMapper<T> rowMapper,
                     final Object... args) throws Exception {
        List<Callable<T>> tasks = new LinkedList<Callable<T>>();

        for (int i = 0; i < parallelism; i++) {
            final String[] taskSqls = Arrays.copyOfRange(sqls, i * queriesPerTask, i * queriesPerTask + queriesPerTask);

            tasks.add(new Callable<T>() {

                public T call() throws Exception {
                    T ret = null;

                    for (int j = 0; j < taskSqls.length; j++) {
                        String sql = taskSqls[j];
                        try {
                            ret = BaseUtils.noArgs(args) ? jdbcTemplate.queryForObject(sql, rowMapper) :
                                    jdbcTemplate.queryForObject(sql, rowMapper, args);
                            logQuery(sql, ret);
                        } catch (EmptyResultDataAccessException e) {
                        }

                        if (ret != null) return ret;
                    }

                    throw new EmptyResultDataAccessException(1);
                }
            });
        }

        try {
            return executorService.invokeAny(tasks);
        } catch (ExecutionException e) {
            throw new EmptyResultDataAccessException(1);
        }
    }


    /**
     * load one without RowMapper
     */
    public Map<String, Object> loadOne(final JdbcTemplate jdbcTemplate, String[] sqls, final Object[] args)
            throws Exception {
        List<Callable<Map<String, Object>>> tasks = new LinkedList<Callable<Map<String, Object>>>();

        for (int i = 0; i < parallelism; i++) {
            final String[] taskSqls = Arrays.copyOfRange(sqls, i * queriesPerTask, i * queriesPerTask + queriesPerTask);

            tasks.add(new Callable<Map<String, Object>>() {

                public Map<String, Object> call() throws Exception {
                    Map<String, Object> ret = null;

                    for (int j = 0; j < taskSqls.length; j++) {
                        String sql = taskSqls[j];
                        try {
                            ret = BaseUtils.noArgs(args) ? jdbcTemplate.queryForMap(sql) :
                                    jdbcTemplate.queryForMap(sql, args);
                            logQuery(sql, ret);
                        } catch (EmptyResultDataAccessException e) {
                        }

                        if (ret != null) return ret;
                    }

                    throw new EmptyResultDataAccessException(1);
                }
            });
        }

        try {
            return executorService.invokeAny(tasks);
        } catch (ExecutionException e) {
            throw new EmptyResultDataAccessException(1);
        }
    }


    /**
     * load with no RowMapper
     */
    public List<Map<String, Object>> load(final JdbcTemplate jdbcTemplate, final String[] sqls,
                                          final SortStrategy<List<Map<String, Object>>> sortStrategy,
                                          final Object... args) throws Exception {
        return forkJoinPool.invoke(
                new ProcessingListQueries<Map<String, Object>>(sqls, jdbcTemplate, args, sortStrategy, queriesPerTask));
    }


    /**
     * load rowset and then mapped with row mapper
     */
    public <E> List<E> loadRowSet(JdbcTemplate jdbcTemplate, String[] sqls, RowMapper<E> rowMapper,
                                  SortStrategy<ResultSetWrappingSqlRowSet> sortStrategy, Object... args) throws Exception {
        ResultSet resultSet = forkJoinPool.invoke(
                new ProcessingRowSetQueries(sqls, jdbcTemplate, args, sortStrategy, queriesPerTask)).getResultSet();
        List<E> ret = Lists.newLinkedList();

        for (int i = 1; resultSet.next(); i++) {
            ret.add(rowMapper.mapRow(resultSet, i));
        }
        return ret;
    }


    /**
     * load count from tables
     */
    public T loadAddable(final JdbcTemplate jdbcTemplate,
                         Class<T> cls,
                         final String[] sqls,
                         final Object... args) throws Exception {
        ProcessingAddableQueries task = null;

        if (cls == Integer.class) {
            task = new ProcessingIntQueries(sqls, jdbcTemplate, args, queriesPerTask);
        } else if (cls == Long.class) {
            task = new ProcessingLongQueries(sqls, jdbcTemplate, args, queriesPerTask);
        } else if (cls == Float.class) {
            task = new ProcessingFloatQueries(sqls, jdbcTemplate, args, queriesPerTask);
        } else if (cls == Double.class) {
            task = new ProcessingDoubleQueries(sqls, jdbcTemplate, args, queriesPerTask);
        } else if (cls == BigInteger.class) {
            task = new ProcessingBigIntegerQueries(sqls, jdbcTemplate, args, queriesPerTask);
        } else if (cls == BigDecimal.class) {
            task = new ProcessingBigDecimalQueries(sqls, jdbcTemplate, args, queriesPerTask);
        }

        if (task != null) return (T) forkJoinPool.invoke(task);

        throw new UnsupportedOperationException("load addable for: " + cls);
    }


    public int update(JdbcTemplate jdbcTemplate, String[] sqls, Object[] args) {
        return forkJoinPool.invoke(new ProcessingUpdate(sqls, jdbcTemplate, args, queriesPerTask));
    }

}
