package function.big_decimal;

import bean.Sql;
import function.FunctionReducer;
import net.sf.jsqlparser.expression.Function;

import java.util.List;
import java.util.Map;

/**
 * Created by guohang.bao on 15/10/29.
 */
public class BigDecimalFunctionReducer extends FunctionReducer<Number> {

    public Number doReduce(List<Number> group, Sql sql) {
        Map.Entry<Function, String> func = sql.functions().entrySet().iterator().next();


        return (Number) BigDecimalFunctionExecutor.createExecutor(func.getKey())
                .execute(func.getKey(), colName(func), group);
    }
}
