package function.timestamp;

import function.FunctionExecutor;
import net.sf.jsqlparser.expression.Function;

import java.sql.Timestamp;

/**
 * Created by guohang.bao on 15/10/29.
 */
public abstract class TimestampFunctionExecutor implements FunctionExecutor<Timestamp, Timestamp> {

    public static TimestampFunctionExecutor createExecutor(Function f) {
        String funcName = f.getName();

        if (funcName.equalsIgnoreCase("max")) {
            return new TimestampMaxFunctionExecutor();
        } else if (funcName.equalsIgnoreCase("min")) {
            return new TimestampMinFunctionExecutor();
        }

        throw new UnsupportedOperationException("unsupported function: " + f.getName());
    }
}
