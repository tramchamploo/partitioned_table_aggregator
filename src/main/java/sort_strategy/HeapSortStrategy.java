package sort_strategy;

import bean.Limit;
import bean.Sql;
import com.google.common.collect.Lists;
import com.google.common.collect.MinMaxPriorityQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

/**
 * Created by guohang.bao on 15-6-25.
 * use a MinMaxPriorityQueue to sort, size is limited with expected rowcount
 */
public class HeapSortStrategy<T> extends ListSortStrategy<T> {

    private MinMaxPriorityQueue<T> heap;

    private Comparator<T> comparator;

    private Logger logger = LoggerFactory.getLogger(HeapSortStrategy.class);


    HeapSortStrategy(Sql sql, Comparator<T> comparator) {
        super(sql);
        this.comparator = comparator;
        if (limit == Limit.none()) {
            heap = MinMaxPriorityQueue.orderedBy(comparator).create();
        } else {
            heap = MinMaxPriorityQueue.orderedBy(comparator).maximumSize(limit.getOffset() + limit.getRowCount()).create();
        }
    }


    /**
     * size is limited, so no need to trim
     */
    public void trim() {
    }


    public synchronized void submit(List<T> result) {
        heap.addAll(result);
    }


    private void logResult(Object result) {
        logger.debug("sort result: {}", result);
    }


    private void logHeap(MinMaxPriorityQueue heap) {
        logger.debug("sort heap: {}", Arrays.toString(heap.toArray()));
    }


    /**
     * heap here is already sorted, just remove the heading useless elements.
     */
    public List<T> result() {
        if (limit instanceof Limit.None) {
            //do nothing
        } else {
            logHeap(heap);
            for (int i = 0; i < limit.getOffset(); i++) {
                heap.removeLast();
            }
        }

        List<T> ret = Lists.newLinkedList();
        while (!heap.isEmpty()) {
            ret.add(0, heap.remove());
        }

        heap.clear();

        logResult(ret);
        return ret;
    }


    public HeapSortStrategy<T> newInstance() {
        return new HeapSortStrategy<T>(sql, comparator);
    }
}
