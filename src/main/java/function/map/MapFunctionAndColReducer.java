package function.map;

import bean.Sql;
import net.sf.jsqlparser.expression.Function;
import org.springframework.util.LinkedCaseInsensitiveMap;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * Created by guohang.bao on 15/10/29.
 */
public class MapFunctionAndColReducer extends MapFunctionReducer {

    @Override
    public Map<String, Object> reduced(List<Map<String, Object>> group, Sql sql) {
        Map<String, Object> functionResult = super.reduced(group, sql);

        LinkedCaseInsensitiveMap<Object> result = new LinkedCaseInsensitiveMap<Object>();

        if (group.size() > 0) {
            Map<String, Object> firstRow = group.get(0);

            List<String> functionReprs = new ArrayList<String>();

            for (Function function : sql.functions().keySet()) {
                functionReprs.add(function.getParameters().getExpressions().get(0).toString());
            }

            Collection<String> aliases = sql.functions().values();

            for (Map.Entry<String, Object> col : firstRow.entrySet()) {
                if (!functionReprs.contains(col.getKey()) && !aliases.contains(col.getKey())) {
                    result.put(col.getKey(), col.getValue());
                }
            }
        }

        result.putAll(functionResult);

        return result;
    }
}
