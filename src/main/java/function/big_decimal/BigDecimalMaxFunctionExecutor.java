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
public class BigDecimalMaxFunctionExecutor extends BigDecimalFunctionExecutor<BigDecimal> {

    public BigDecimal execute(Function f, String colName, final List<BigDecimal> group) {
        // support only one parameter currently
        Expression expr = f.getParameters().getExpressions().get(0);

        final BigDecimalHolder holder = new BigDecimalHolder();

        expr.accept(new ExpressionVisitorAdapter() {
                        @Override
                        public void visit(Column column) {
                            for (int i = 0; i < group.size(); i++) {
                                BigDecimal colValue = group.get(i);

                                if (i == 0) {
                                    holder.set(colValue);
                                } else if (colValue.compareTo(holder.get()) > 0) {
                                    holder.set(colValue);
                                }
                            }
                        }
                    }
        );
        return holder.get();
    }
}
