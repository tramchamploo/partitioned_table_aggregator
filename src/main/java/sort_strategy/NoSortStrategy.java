package sort_strategy;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

/**
 * Created by guohang.bao on 15-6-25.
 * used when no need to order, so just add sub query result and return
 */
public class NoSortStrategy<E> implements SortStrategy<List<E>> {

    private List<E> result;


    public void submit(List<E> result) {
        if (this.result == null) {
            this.result = Collections.synchronizedList(new LinkedList<E>());
        }
        this.result.addAll(result);
    }


    public List<E> result() {
        ArrayList<E> ret = new ArrayList<E>(result);
        result.clear();
        return ret;
    }


    public NoSortStrategy<E> newInstance() {
        return new NoSortStrategy<E>();
    }


    public void trim() {
    }

}
