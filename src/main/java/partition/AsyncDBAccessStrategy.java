package partition;

import com.google.common.util.concurrent.FutureCallback;
import common.PublicExecutorService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;

import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by guohang.bao on 15-6-25.
 */
public class AsyncDBAccessStrategy<T, E extends Comparable<E>> implements DBAccessStrategy<T, E> {

    private Logger logger = LoggerFactory.getLogger(AsyncDBAccessStrategy.class);

    private PublicExecutorService executorService;
    private CountDownLatch countDownLatch;

    public AsyncDBAccessStrategy(PublicExecutorService executorService) {
        this.executorService = executorService;
    }

    private boolean noArgs(Object... args) {
        return args.length == 0;
    }

    private void logQuery(String sql, Object result) {
        logger.debug("subsql sql execution: {}, {}", sql, result);
    }

    @Override
    public List<T> load(final JdbcTemplate jdbcTemplate, final String[] sqls, final RowMapper<T> rowMapper,
                        final SortStrategy<T, E> sortStrategy, final Object... args) throws Exception {
        countDownLatch = new CountDownLatch(sqls.length);
        for (int i = 0; i < sqls.length; i++) {
            final int finalI = i;
            final String sql = sqls[finalI];
            executorService.submit(new Callable<List<T>>() {
                public List<T> call() throws Exception {
                    return noArgs(args) ? jdbcTemplate.query(sql, rowMapper) :
                            jdbcTemplate.query(sql, rowMapper, args);
                }
            }, new FutureCallback<List<T>>() {
                public void onSuccess(List<T> result) {
                    sortStrategy.submit(result);
                    countDownLatch.countDown();
                    logQuery(sql, result);
                }

                public void onFailure(Throwable t) {
                    logger.error("subquery failed:", t);
                }
            });
        }
        countDownLatch.await();
        return sortStrategy.result();
    }

    public class SingleResultWrapper {

        private T value;

        public T getValue() {
            return value;
        }

        public void setValue(T value) {
            this.value = value;
        }
    }

    @Override
    public T loadOne(final JdbcTemplate jdbcTemplate, final String[] sqls, final RowMapper<T> rowMapper,
                     final Object... args) throws Exception {
        countDownLatch = new CountDownLatch(1);
        final SingleResultWrapper wrapper = new SingleResultWrapper();
        for (int i = 0; i < sqls.length; i++) {
            final int finalI = i;
            final String sql = sqls[finalI];
            executorService.submit(new Callable<T>() {
                public T call() throws Exception {
                    return noArgs(args) ? jdbcTemplate.queryForObject(sql, rowMapper) :
                            jdbcTemplate.queryForObject(sql, rowMapper, args);
                }
            }, new FutureCallback<T>() {
                public void onSuccess(T result) {
                    countDownLatch.countDown();
                    wrapper.setValue(result);
                    logQuery(sql, result);
                }

                public void onFailure(Throwable t) {
                    if (t instanceof EmptyResultDataAccessException) {
                    } else {
                        logger.error("subquery failed:", t);
                    }
                }
            });
        }
        countDownLatch.await();
        return wrapper.getValue();
    }

    @Override
    public List<Map<String, Object>> load(final JdbcTemplate jdbcTemplate, final String[] sqls,
                                          final SortStrategy<Map<String, Object>, E> sortStrategy, final Object... args) throws Exception {
        countDownLatch = new CountDownLatch(sqls.length);
        for (int i = 0; i < sqls.length; i++) {
            final int finalI = i;
            final String sql = sqls[finalI];
            executorService.submit(new Callable<List<Map<String, Object>>>() {
                public List<Map<String, Object>> call() throws Exception {
                    return noArgs(args) ? jdbcTemplate.queryForList(sql) :
                            jdbcTemplate.queryForList(sql, args);
                }
            }, new FutureCallback<List<Map<String, Object>>>() {
                public void onSuccess(List<Map<String, Object>> result) {
                    sortStrategy.submit(result);
                    countDownLatch.countDown();
                    logQuery(sql, result);
                }

                public void onFailure(Throwable t) {
                    logger.error("subquery failed:", t);
                }
            });
        }
        countDownLatch.await();
        return sortStrategy.result();
    }

    @Override
    public Long loadCount(final JdbcTemplate jdbcTemplate, final String[] sqls, final Object... args) throws Exception {
        final AtomicLong ret = new AtomicLong(0l);
        countDownLatch = new CountDownLatch(sqls.length);
        for (int i = 0; i < sqls.length; i++) {
            final int finalI = i;
            executorService.submit(new Callable<Long>() {
                public Long call() throws Exception {
                    return noArgs(args) ? jdbcTemplate.queryForObject(sqls[finalI], Long.class) :
                            jdbcTemplate.queryForObject(sqls[finalI], Long.class, args);
                }
            }, new FutureCallback<Long>() {
                public void onSuccess(Long cnt) {
                    ret.getAndAdd(cnt);
                    countDownLatch.countDown();
                }

                public void onFailure(Throwable t) {
                    logger.error("subquery failed:", t);
                }
            });
        }
        countDownLatch.await();
        long cnt = ret.get();
        logger.debug("loadCount result: {}", cnt);
        return cnt;
    }

    @Override
    public List<E> loadSelfComparable(final JdbcTemplate jdbcTemplate, final String[] sqls, final RowMapper<E> rowMapper,
                                      final SortStrategy<T, E> sortStrategy, final Object... args) throws Exception {
        countDownLatch = new CountDownLatch(sqls.length);
        for (int i = 0; i < sqls.length; i++) {
            final int finalI = i;
            final String sql = sqls[finalI];
            executorService.submit(new Callable<List<E>>() {
                public List<E> call() throws Exception {
                    return noArgs(args) ? jdbcTemplate.query(sql, rowMapper) :
                            jdbcTemplate.query(sql, rowMapper, args);
                }
            }, new FutureCallback<List<E>>() {
                public void onSuccess(List<E> result) {
                    sortStrategy.submitComparable(result);
                    countDownLatch.countDown();
                    logQuery(sql, result);
                }

                public void onFailure(Throwable t) {
                    logger.error("subquery failed:", t);
                }
            });
        }
        countDownLatch.await();
        return sortStrategy.selfComparableResult();
    }
}
