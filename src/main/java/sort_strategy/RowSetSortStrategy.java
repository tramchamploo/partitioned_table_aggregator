package sort_strategy;

import bean.Sql;
import org.springframework.jdbc.support.rowset.ResultSetWrappingSqlRowSet;
import org.springframework.jdbc.support.rowset.SqlRowSet;

import java.util.Comparator;

/**
 * Created by guohang.bao on 15-8-27.
 */
public class RowSetSortStrategy extends SqlBasedSortStrategy<ResultSetWrappingSqlRowSet> {

    private ResultSetWrappingSqlRowSet left;

    private ResultSetWrappingSqlRowSet right;

    private Comparator<SqlRowSet> comparator;

    public RowSetSortStrategy(Sql sql, Comparator<SqlRowSet> comparator) {
        super(sql);
        this.comparator = comparator;
    }

    public void submit(ResultSetWrappingSqlRowSet result) {
        if (left == null) {
            left = result;
        } else {
            right = result;
        }
    }

    public ResultSetWrappingSqlRowSet result() {
        MergeSortSqlRowSet ret = new MergeSortSqlRowSet(left, right, comparator);

        left = null;
        right = null;

        return ret;
    }

    public SortStrategy<ResultSetWrappingSqlRowSet> newInstance() {
        return new RowSetSortStrategy(sql, comparator);
    }

    public void trim() {
    }
}
