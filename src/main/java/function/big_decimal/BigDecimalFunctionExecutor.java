package function.big_decimal;

import function.FunctionExecutor;
import net.sf.jsqlparser.expression.Function;

import java.math.BigDecimal;

/**
 * Created by guohang.bao on 15/10/29.
 */
public abstract class BigDecimalFunctionExecutor<R extends Number> implements FunctionExecutor<BigDecimal, R> {

    public static BigDecimalFunctionExecutor createExecutor(Function f) {
        String funcName = f.getName();

        if (funcName.equalsIgnoreCase("avg")) {
            return new BigDecimalAvgFunctionExecutor();
        } else if (funcName.equalsIgnoreCase("max")) {
            return new BigDecimalMaxFunctionExecutor();
        } else if (funcName.equalsIgnoreCase("min")) {
            return new BigDecimalMinFunctionExecutor();
        }

        throw new UnsupportedOperationException("unsupported function: " + f.getName());
    }
}
