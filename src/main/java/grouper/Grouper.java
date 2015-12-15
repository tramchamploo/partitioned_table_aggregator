package grouper;

import bean.Sql;
import util.tuple.Tuple;

import java.util.List;
import java.util.Map;

/**
 * Created by guohang.bao on 15/10/28.
 */
public abstract class Grouper<T> {

    public List<T> group(List<T> unGrouped, Sql sql) {
        if (sql.groupBys() == null) return unGrouped;

        Map<List<Tuple<String, Object>>, List<T>> grouped = doGroup(unGrouped, sql);
        return doCombine(grouped, sql);
    }


    protected abstract Map<List<Tuple<String, Object>>, List<T>> doGroup(List<T> unGrouped, Sql sql);


    protected abstract List<T> doCombine(Map<List<Tuple<String, Object>>, List<T>> grouped, Sql sql);


}
