package function;

import bean.Sql;
import net.sf.jsqlparser.expression.Function;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * Created by guohang.bao on 15/10/29.
 */
public abstract class FunctionReducer<T> {

    protected abstract T doReduce(List<T> group, Sql sql);

    public T reduced(List<T> group, Sql sql) {
        List<T> nullFiltered = new LinkedList<T>();

        for (T n : group) {
            if (n != null) nullFiltered.add(n);
        }

        return doReduce(nullFiltered, sql);
    }

    protected String colName(Map.Entry<Function, String> func) {
        return func.getValue() == null ?
                func.getKey().getParameters().getExpressions().get(0).toString()
                : func.getValue();
    }
}
