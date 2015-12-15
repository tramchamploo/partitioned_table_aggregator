package function.map;

import bean.Sql;
import function.FunctionReducer;
import com.google.common.collect.Maps;
import net.sf.jsqlparser.expression.Function;

import java.util.List;
import java.util.Map;

/**
 * Created by guohang.bao on 15/10/29.
 */
public class MapFunctionReducer extends FunctionReducer<Map<String, Object>> {

    @Override
    protected Map<String, Object> doReduce(List<Map<String, Object>> group, Sql sql) {
        return null;
    }

    @Override
    public Map<String, Object> reduced(List<Map<String, Object>> group, Sql sql) {

        Map<String, Object> ret = Maps.newHashMapWithExpectedSize(8);

        for (Map.Entry<Function, String> f : sql.functions().entrySet()) {
            Object executeResult = MapFunctionExecutor.createExecutor(f.getKey())
                    .execute(f.getKey(), colName(f), group);

            if (f.getValue() != null) { // if have alias, put alias as key
                ret.put(f.getValue(), executeResult);
            } else {
                ret.put(f.getKey().toString(), executeResult);
            }
        }

        return ret;
    }
}
