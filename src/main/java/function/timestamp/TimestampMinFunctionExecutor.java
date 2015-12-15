package function.timestamp;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.ExpressionVisitorAdapter;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.schema.Column;

import java.sql.Timestamp;
import java.util.List;

/**
 * Created by guohang.bao on 15/10/29.
 */
public class TimestampMinFunctionExecutor extends TimestampFunctionExecutor {

    public Timestamp execute(Function f, String colName, final List<Timestamp> group) {
        // support only one parameter currently
        Expression expr = f.getParameters().getExpressions().get(0);

        final TimestampHolder holder = new TimestampHolder();

        expr.accept(new ExpressionVisitorAdapter() {
                        @Override
                        public void visit(Column column) {
                            for (int i = 0; i < group.size(); i++) {
                                Timestamp colValue = group.get(i);

                                if (i == 0) {
                                    holder.set(colValue);
                                } else if (colValue.compareTo(holder.get()) < 0) {
                                    holder.set(colValue);
                                }
                            }
                        }
                    }
        );
        return holder.get();
    }
}
