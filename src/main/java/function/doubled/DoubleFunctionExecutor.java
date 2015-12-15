package function.doubled;

import function.FunctionExecutor;
import net.sf.jsqlparser.expression.Function;

/**
 * Created by guohang.bao on 15/10/29.
 */
public abstract class DoubleFunctionExecutor<R extends Number> implements FunctionExecutor<Double, R> {

    public static DoubleFunctionExecutor createExecutor(Function f) {
        String funcName = f.getName();

        if (funcName.equalsIgnoreCase("avg")) {
            return new DoubleAvgFunctionExecutor();
        } else if (funcName.equalsIgnoreCase("max")) {
            return new DoubleMaxFunctionExecutor();
        } else if (funcName.equalsIgnoreCase("min")) {
            return new DoubleMinFunctionExecutor();
        }

        throw new UnsupportedOperationException("unsupported function: " + f.getName());
    }
}
