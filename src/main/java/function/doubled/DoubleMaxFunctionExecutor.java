package function.doubled;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.ExpressionVisitorAdapter;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.schema.Column;

import java.util.List;

/**
 * Created by guohang.bao on 15/10/29.
 */
public class DoubleMaxFunctionExecutor extends DoubleFunctionExecutor<Double> {

    public Double execute(Function f, String colName, final List<Double> group) {
        // support only one parameter currently
        Expression expr = f.getParameters().getExpressions().get(0);

        final DoubleHolder holder = new DoubleHolder();

        expr.accept(new ExpressionVisitorAdapter() {
                        @Override
                        public void visit(Column column) {
                            for (int i = 0; i < group.size(); i++) {
                                Double colValue = group.get(i);

                                if (i == 0) {
                                    holder.set(colValue);
                                } else if (colValue > holder.get()) {
                                    holder.set(colValue);
                                }
                            }
                        }
                    }
        );
        return holder.get();
    }
}
