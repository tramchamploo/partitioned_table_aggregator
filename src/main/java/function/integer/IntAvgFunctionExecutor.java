package function.integer;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.ExpressionVisitorAdapter;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.schema.Column;

import java.math.BigDecimal;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by guohang.bao on 15/10/29.
 */
public class IntAvgFunctionExecutor extends IntFunctionExecutor<Double> {

    public Double execute(Function f, final String colName, final List<Integer> group) {
        // support only one parameter currently
        Expression expr = f.getParameters().getExpressions().get(0);

        // use atomic because only final field can be in nested class
        final AtomicLong ret = new AtomicLong();

        expr.accept(new ExpressionVisitorAdapter() {
            @Override
            public void visit(Column column) {
                for (Integer i : group) {
                    ret.addAndGet(i);
                }
            }
        });

        BigDecimal bd = new BigDecimal(ret.longValue() / (group.size() + 0.0));
        return bd.setScale(4, BigDecimal.ROUND_HALF_UP).doubleValue();
    }
}
