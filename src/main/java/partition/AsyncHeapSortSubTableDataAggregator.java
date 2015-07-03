package partition;

import cn.j.rio.tools.PublicExecutorService;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;

import java.util.Comparator;
import java.util.List;
import java.util.Map;

/**
 * Created by guohang.bao on 15-6-25.
 */
public final class AsyncHeapSortSubTableDataAggregator<T, E extends Comparable<E>>
        extends SubTableDataAggregator<T, E> {

    private PublicExecutorService executorService;

    private Comparator<T> comparator;

    private static final Comparator<SomeComparator> SOME_COMPARATOR = new SomeComparator();

    private AsyncHeapSortSubTableDataAggregator(JdbcTemplate jdbcTemplate,
                                                PublicExecutorService executorService,
                                                Sql sql, Comparator<T> comparator) {
        super(jdbcTemplate, sql);
        this.executorService = executorService;
        this.comparator = comparator;
    }

    public static <T> AsyncHeapSortSubTableDataAggregator<T, SomeComparable> of(JdbcTemplate jdbcTemplate,
                                                                                PublicExecutorService executorService,
                                                                                Sql sql, Comparator<T> comparator) {
        return new AsyncHeapSortSubTableDataAggregator<T, SomeComparable>(jdbcTemplate, executorService, sql, comparator);
    }

    public static <T> AsyncHeapSortSubTableDataAggregator<T, SomeComparable> of(JdbcTemplate jdbcTemplate,
                                                                                PublicExecutorService executorService,
                                                                                Sql sql, Class<T> clz) {
        return new AsyncHeapSortSubTableDataAggregator<T, SomeComparable>(jdbcTemplate, executorService, sql, new Comparator<T>() {
            @Override
            public int compare(T o1, T o2) {
                throw new UnsupportedOperationException();
            }
        });
    }

    public static <E extends Comparable<E>> AsyncHeapSortSubTableDataAggregator<SomeComparator, E> of(JdbcTemplate jdbcTemplate,
                                                                                                      PublicExecutorService executorService,
                                                                                                      Sql sql) {
        return new AsyncHeapSortSubTableDataAggregator<SomeComparator, E>(jdbcTemplate, executorService, sql, SOME_COMPARATOR);
    }

    public static Comparator<SomeComparator> anyComparator() {
        return SOME_COMPARATOR;
    }

    @Override
    public List<T> run(RowMapper<T> rowMapper, Object... args) throws Exception {
        return comparator == SOME_COMPARATOR ? super.run(new AsyncDBAccessStrategy<T, E>(executorService),
                new NoSortStrategy<T, E>(), rowMapper, args) :
                super.run(new AsyncDBAccessStrategy<T, E>(executorService),
                        new HeapSortStrategy<T, E>(sql, comparator), rowMapper, args);
    }

    @Override
    public List<Map<String, Object>> run(Object... args) throws Exception {
        return comparator == SOME_COMPARATOR ? super.run(new AsyncDBAccessStrategy<T, E>(executorService),
                new NoSortStrategy<T, E>(), args) :
                super.run(new AsyncDBAccessStrategy<T, E>(executorService),
                        new HeapSortStrategy<T, E>(sql, comparator), args);
    }

    @Override
    public T runForOne(RowMapper<T> rowMapper, Object... args) throws Exception {
        return super.runForOne(new AsyncDBAccessStrategy<T, E>(executorService), rowMapper, args);
    }

    @Override
    public Long runCount(Object... args) throws Exception {
        return super.runCount(new AsyncDBAccessStrategy<T, E>(executorService), args);
    }

    @Override
    public List<E> runForComparable(RowMapper<E> rowMapper, Object... args) throws Exception {
        return super.runForComparable(new AsyncDBAccessStrategy<T, E>(executorService), new HeapSortStrategy<T, E>(sql, comparator), rowMapper, args);
    }

    public static class SomeComparable implements Comparable<SomeComparable> {
        @Override
        public int compareTo(SomeComparable o) {
            throw new UnsupportedOperationException();
        }
    }

    public static class SomeComparator implements Comparator<SomeComparator> {
        @Override
        public int compare(SomeComparator o1, SomeComparator o2) {
            throw new UnsupportedOperationException();
        }
    }


}
