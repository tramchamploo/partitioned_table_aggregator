package function.map;

import function.FunctionExecutor;
import net.sf.jsqlparser.expression.Function;

import java.util.Map;

/**
 * Created by guohang.bao on 15/10/29.
 */
public abstract class MapFunctionExecutor<R> implements FunctionExecutor<Map<String, Object>, R> {

    public static MapFunctionExecutor createExecutor(Function f) {
        String funcName = f.getName();

        if (funcName.equalsIgnoreCase("sum")) {
            return new MapSumFunctionExecutor();
        } else if (funcName.equalsIgnoreCase("count")) {
            return new MapCountFunctionExecutor();
        } else if (funcName.equalsIgnoreCase("avg")) {
            return new MapAvgFunctionExecutor();
        } else if (funcName.equalsIgnoreCase("max")) {
            return new MapMaxFunctionExecutor();
        } else if (funcName.equalsIgnoreCase("min")) {
            return new MapMinFunctionExecutor();
        }

        throw new UnsupportedOperationException("unsupported function: " + f.getName());
    }
}
