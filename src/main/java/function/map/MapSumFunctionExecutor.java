package function.map;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.ExpressionVisitorAdapter;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.schema.Column;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;

/**
 * Created by guohang.bao on 15/10/29.
 */
public class MapSumFunctionExecutor extends MapFunctionExecutor<Number> {

    public Number execute(Function f, final String colName, final List<Map<String, Object>> group) {
        // support only one parameter currently
        Expression expr = f.getParameters().getExpressions().get(0);

        final BigDecimalHolder holder = new BigDecimalHolder();
        holder.set(new BigDecimal(0));

        final ClassHolder clsHolder = new ClassHolder();

        expr.accept(new ExpressionVisitorAdapter() {
            @Override
            public void visit(Column column) {
                for (Map<String, Object> row : group) {
                    Object element = row.get(colName);

                    if (clsHolder.get() == null) clsHolder.set(element.getClass());

                    holder.set(holder.get().add(new BigDecimal(element.toString())));
                }
            }
        });

        Class<?> cls = clsHolder.get();
        if (cls == Integer.class || cls == Long.class) {
            return holder.get().longValue();
        } else {
            return holder.get();
        }
    }
}
