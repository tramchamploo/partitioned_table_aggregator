package function.map;

import function.FunctionExecutor;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.ExpressionVisitorAdapter;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.schema.Column;

import java.util.Comparator;
import java.util.List;
import java.util.Map;

/**
 * Created by guohang.bao on 15/10/29.
 */
public class MapMinFunctionExecutor extends MapFunctionExecutor<Object> {

    public Object execute(Function f, final String colName, final List<Map<String, Object>> group) {
        // support only one parameter currently
        Expression expr = f.getParameters().getExpressions().get(0);

        final FunctionExecutor.ObjectHolder holder = new FunctionExecutor.ObjectHolder();

        expr.accept(new ExpressionVisitorAdapter() {
            @Override
            public void visit(Column column) {
                Comparator comparator = null;

                for (int i = 0; i < group.size(); i++) {
                    Map<String, Object> row = group.get(i);
                    Object colValue = row.get(colName);

                    if (i == 0 && Comparable.class.isAssignableFrom(colValue.getClass())) {
                        comparator = new Comparator() {
                            public int compare(Object o1, Object o2) {
                                Comparable d1 = (Comparable) o1;
                                Comparable d2 = (Comparable) o2;
                                return d1.compareTo(d2);
                            }
                        };

                        holder.set(colValue);
                    } else if (i == 0) {
                        throw new RuntimeException("value cannot be compared, map: " + row + " field: " + colName);
                    } else {
                        if (comparator.compare(holder.get(), colValue) > 0) holder.set(colValue);
                    }
                }
            }
        });
        return holder.get();
    }
}
