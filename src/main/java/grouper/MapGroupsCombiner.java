package grouper;

import bean.Sql;
import function.map.MapFunctionReducer;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import util.tuple.Tuple;

import java.util.List;
import java.util.Map;

/**
 * Created by guohang.bao on 15/10/29.
 */
public class MapGroupsCombiner implements GroupsCombiner<Map<String, Object>> {

    private static final MapGroupsCombiner instance = new MapGroupsCombiner();

    public static MapGroupsCombiner getInstance() {
        return instance;
    }

    public List<Map<String, Object>> combine(Map<List<Tuple<String, Object>>, List<Map<String, Object>>> grouped, Sql sql) {

        List<Map<String, Object>> ret = Lists.newLinkedList();

        for (Map.Entry<List<Tuple<String, Object>>, List<Map<String, Object>>> group : grouped.entrySet()) {
            List<Tuple<String, Object>> byColumns = group.getKey();
            List<Map<String, Object>> groupContent = group.getValue();

            Map<String, Object> row = Maps.newHashMapWithExpectedSize(8);

            // put group by columns
            for (Tuple<String, Object> col : byColumns) {
                String alias = sql.aliasByField(col._1());

                row.put(alias == null ? col._1() : alias, col._2());
            }

            // function columns
            Map<String, Object> funcResult = new MapFunctionReducer().reduced(groupContent, sql);
            row.putAll(funcResult);

            ret.add(row);
        }

        return ret;
    }
}
