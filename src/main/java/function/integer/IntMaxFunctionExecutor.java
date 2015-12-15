package function.integer;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.ExpressionVisitorAdapter;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.schema.Column;

import java.util.List;

/**
 * Created by guohang.bao on 15/10/29.
 */
public class IntMaxFunctionExecutor extends IntFunctionExecutor<Integer> {

    public Integer execute(Function f, String colName, final List<Integer> group) {
        // support only one parameter currently
        Expression expr = f.getParameters().getExpressions().get(0);

        final IntHolder holder = new IntHolder();

        expr.accept(new ExpressionVisitorAdapter() {
                        @Override
                        public void visit(Column column) {
                            for (int i = 0; i < group.size(); i++) {
                                Integer colValue = group.get(i);

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
