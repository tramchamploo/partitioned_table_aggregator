package function.timestamp;

import bean.Sql;
import function.FunctionReducer;
import net.sf.jsqlparser.expression.Function;

import java.sql.Timestamp;
import java.util.List;
import java.util.Map;

/**
 * Created by guohang.bao on 15/10/29.
 */
public class TimestampFunctionReducer extends FunctionReducer<Timestamp> {

    @Override
    protected Timestamp doReduce(List<Timestamp> group, Sql sql) {
        Map.Entry<Function, String> func = sql.functions().entrySet().iterator().next();

        return TimestampFunctionExecutor.createExecutor(func.getKey())
                .execute(func.getKey(), colName(func), group);
    }
}
