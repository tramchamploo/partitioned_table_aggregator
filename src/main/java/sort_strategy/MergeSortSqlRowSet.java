package sort_strategy;

import org.springframework.jdbc.InvalidResultSetAccessException;
import org.springframework.jdbc.support.rowset.ResultSetWrappingSqlRowSet;
import org.springframework.jdbc.support.rowset.SqlRowSet;

import java.sql.ResultSet;
import java.util.Comparator;

/**
 * Created by guohang.bao on 15-8-27.
 */
public class MergeSortSqlRowSet extends ResultSetWrappingSqlRowSet {

    public MergeSortSqlRowSet(ResultSet resultSet) throws InvalidResultSetAccessException {
        super(resultSet);
    }

    public MergeSortSqlRowSet(ResultSetWrappingSqlRowSet left, ResultSetWrappingSqlRowSet right, Comparator<SqlRowSet> comparator)
            throws InvalidResultSetAccessException {
        this(new MergeSortResultSet(left.getResultSet(), right.getResultSet(), comparator));
    }

}
