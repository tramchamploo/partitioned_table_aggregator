package grouper;

import bean.Sql;
import util.tuple.Tuple;

import java.util.List;
import java.util.Map;

/**
 * Created by guohang.bao on 15/10/29.
 */
public interface GroupsCombiner<T> {

    List<T> combine(Map<List<Tuple<String, Object>>, List<T>> grouped, Sql sql);

}
