package function.longl;

import function.FunctionExecutor;
import net.sf.jsqlparser.expression.Function;

/**
 * Created by guohang.bao on 15/10/29.
 */
public abstract class LongFunctionExecutor<R extends Number> implements FunctionExecutor<Long, R> {

    public static LongFunctionExecutor createExecutor(Function f) {
        String funcName = f.getName();

        if (funcName.equalsIgnoreCase("avg")) {
            return new LongAvgFunctionExecutor();
        } else if (funcName.equalsIgnoreCase("max")) {
            return new LongMaxFunctionExecutor();
        } else if (funcName.equalsIgnoreCase("min")) {
            return new LongMinFunctionExecutor();
        }

        throw new UnsupportedOperationException("unsupported function: " + f.getName());
    }
}
