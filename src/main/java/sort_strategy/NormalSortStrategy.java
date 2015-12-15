package sort_strategy;

import bean.Sql;
import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

/**
 * Created by guohang.bao on 15-6-25.
 */
public class NormalSortStrategy<T> extends ListSortStrategy<T> {

    private Comparator<T> comparator;

    private Logger logger = LoggerFactory.getLogger(NormalSortStrategy.class);

    private List<T> data;

    private static final int DEFAULT_CAPACITY = 10;

    private int nSubmitted = 0;

    private boolean sorted = false;


    public NormalSortStrategy(Sql sql, Comparator<T> comparator) {
        super(sql);
        this.comparator = comparator;
        int expectedSize = Math.max(DEFAULT_CAPACITY,
                sql.limit().count() / sql.partitionedTables()[0].getTableNames().length * 2);
        data = Collections.synchronizedList(new ArrayList<T>(expectedSize));
    }


    public void trim() {
        if (sql.groupBys() == null && limit.count() != 0) {
            data = Lists.newArrayList(data.subList(0, Math.min(data.size(), limit.count())));
        }
    }


    public void submit(List<T> result) {
        if (result != null) {
            data.addAll(result);

            if (++nSubmitted > 1) {
                Collections.sort(data, comparator);
                sorted = true;
            }
        }
    }


    private void logResult(Object result) {
        logger.debug("sort result: {}", result);
    }


    public List<T> result() {
        if (!sorted) Collections.sort(data, comparator);

        logResult(data);
        ArrayList<T> ret = new ArrayList<T>(data);

        data.clear();
        sorted = false;
        nSubmitted = 0;

        return ret;
    }


    public ListSortStrategy<T> newInstance() {
        return new NormalSortStrategy<T>(sql, comparator);
    }
}
