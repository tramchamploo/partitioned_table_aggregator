package function.doubled;

import com.google.common.util.concurrent.AtomicDouble;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.ExpressionVisitorAdapter;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.schema.Column;

import java.math.BigDecimal;
import java.util.List;

/**
 * Created by guohang.bao on 15/10/29.
 */
public class DoubleAvgFunctionExecutor extends DoubleFunctionExecutor<Double> {

    public Double execute(Function f, final String colName, final List<Double> group) {
        // support only one parameter currently
        Expression expr = f.getParameters().getExpressions().get(0);

        // use atomic because only final field can be in nested class
        final AtomicDouble ret = new AtomicDouble();

        expr.accept(new ExpressionVisitorAdapter() {
            @Override
            public void visit(Column column) {
                for (Double i : group) {
                    ret.addAndGet(i);
                }
            }
        });

        BigDecimal bd = new BigDecimal(ret.doubleValue() / group.size());
        return bd.setScale(4, BigDecimal.ROUND_HALF_UP).doubleValue();
    }

}
