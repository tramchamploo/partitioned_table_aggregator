package function.big_decimal;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.ExpressionVisitorAdapter;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.schema.Column;

import java.math.BigDecimal;
import java.util.List;

/**
 * Created by guohang.bao on 15/10/29.
 */
public class BigDecimalAvgFunctionExecutor extends BigDecimalFunctionExecutor<BigDecimal> {

    public BigDecimal execute(Function f, final String colName, final List<BigDecimal> group) {
        // support only one parameter currently
        Expression expr = f.getParameters().getExpressions().get(0);


        final BigDecimalHolder holder = new BigDecimalHolder();
        holder.set(new BigDecimal(0));

        expr.accept(new ExpressionVisitorAdapter() {
            @Override
            public void visit(Column column) {
                for (BigDecimal i : group) {
                    holder.set(holder.get().add(i));
                }
            }
        });
        return holder.get().divide(new BigDecimal(group.size()), 4, BigDecimal.ROUND_HALF_UP);
    }

}
