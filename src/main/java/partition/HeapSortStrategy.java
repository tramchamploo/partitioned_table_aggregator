package partition;

import com.google.common.collect.Lists;
import com.google.common.collect.MinMaxPriorityQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

/**
 * Created by guohang.bao on 15-6-25.
 */
public class HeapSortStrategy<T, E extends Comparable<E>> implements SortStrategy<T, E> {

    private MinMaxPriorityQueue<E> selfOrderedHeap;

    private MinMaxPriorityQueue<T> heap;

    private boolean selfOrdered;

    private Limit limit;

    private Logger logger = LoggerFactory.getLogger(HeapSortStrategy.class);

    HeapSortStrategy(Sql sql) {
        this.limit = sql.limit();
        selfOrderedHeap = MinMaxPriorityQueue.maximumSize(limit.getOffset() + limit.getRowCount()).create();
        selfOrdered = true;
    }

    HeapSortStrategy(Sql sql, Comparator<T> comparator) {
        this.limit = sql.limit();
        heap = MinMaxPriorityQueue.orderedBy(comparator).maximumSize(limit.getOffset() + limit.getRowCount()).create();
    }

    @Override
    public synchronized void submit(List<T> result) {
        if (!selfOrdered) {
            heap.addAll(result);
        } else {
            throw new UnsupportedOperationException("Must specify comparator");
        }
    }

    @Override
    public synchronized void submitComparable(List<E> result) {
        if (selfOrdered) {
            selfOrderedHeap.addAll(result);
        } else {
            throw new UnsupportedOperationException("Element must be comparable");
        }
    }

    private void logResult(Object result) {
        logger.debug("sort result: {}", result);
    }

    private void logHeap(MinMaxPriorityQueue heap) {
        logger.debug("sort heap: {}", Arrays.toString(heap.toArray()));
    }

    @Override
    public List<T> result() {
        if (selfOrdered) {
            throw new UnsupportedOperationException();
        }
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
        logResult(ret);
        return ret;
    }

    @Override
    public List<E> selfComparableResult() {
        if (!selfOrdered) {
            throw new UnsupportedOperationException();
        }
        if (limit instanceof Limit.None) {
            //do nothing
        } else {
            logHeap(selfOrderedHeap);
            for (int i = 0; i < limit.getOffset(); i++) {
                selfOrderedHeap.removeLast();
            }
        }
        List<E> ret = Lists.newLinkedList();
        while (!selfOrderedHeap.isEmpty()) {
            ret.add(0, selfOrderedHeap.remove());
        }
        logResult(ret);
        return ret;
    }
}
