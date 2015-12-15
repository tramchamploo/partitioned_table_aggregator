package function.map;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.ExpressionVisitorAdapter;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.schema.Column;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by guohang.bao on 15/10/29.
 */
public class MapCountFunctionExecutor extends MapFunctionExecutor<Long> {

    public Long execute(Function f, final String colName, final List<Map<String, Object>> group) {
        // support only one parameter currently
        Expression expr = f.getParameters().getExpressions().get(0);

        // use atomic because only final field can be in nested class
        final AtomicLong ret = new AtomicLong();

        expr.accept(new ExpressionVisitorAdapter() {

            private void addToResult() {
                for (Map<String, Object> row : group) {
                    if (row.get(colName) != null) {
                        ret.addAndGet(1);
                    }
                }
            }

            @Override
            public void visit(Column column) {
                addToResult();
            }

            @Override
            public void visit(LongValue value) {
                addToResult();
            }

        });
        return ret.longValue();
    }
}
