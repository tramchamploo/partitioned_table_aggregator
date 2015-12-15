package function.integer;

import function.FunctionExecutor;
import net.sf.jsqlparser.expression.Function;

/**
 * Created by guohang.bao on 15/10/29.
 */
public abstract class IntFunctionExecutor<R extends Number> implements FunctionExecutor<Integer, R> {

    public static IntFunctionExecutor createExecutor(Function f) {
        String funcName = f.getName();

        if (funcName.equalsIgnoreCase("avg")) {
            return new IntAvgFunctionExecutor();
        } else if (funcName.equalsIgnoreCase("max")) {
            return new IntMaxFunctionExecutor();
        } else if (funcName.equalsIgnoreCase("min")) {
            return new IntMinFunctionExecutor();
        }

        throw new UnsupportedOperationException("unsupported function: " + f.getName());
    }
}
