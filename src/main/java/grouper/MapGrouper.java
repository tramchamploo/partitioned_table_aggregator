package grouper;

import bean.Sql;
import com.google.common.base.Function;
import com.google.common.collect.Lists;
import net.sf.jsqlparser.schema.Column;
import util.BaseUtils;
import util.tuple.Tuple;
import util.tuple.Tuples;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * Created by guohang.bao on 15/10/28.
 */
public class MapGrouper extends Grouper<Map<String, Object>> {

    @Override
    public Map<List<Tuple<String, Object>>, List<Map<String, Object>>> doGroup(List<Map<String, Object>> unGrouped,
                                                                               final Sql sql) {
        List<Column> byColumns = sql.groupBys();

        final List<String> columnNames = Lists.transform(byColumns, new Function<Column, String>() {
            public String apply(Column c) {
                return c.getColumnName();
            }
        });

        return BaseUtils.groupBy(unGrouped, new Function<Map<String, Object>, List<Tuple<String, Object>>>() {
            public List<Tuple<String, Object>> apply(Map<String, Object> row) {
                List<Tuple<String, Object>> key = new LinkedList<Tuple<String, Object>>();

                for (int i = 0; i < columnNames.size(); i++) {
                    String colName = columnNames.get(i);

                    String alias = sql.aliasByField(colName);

                    key.add(Tuples.of(colName, row.get(alias) == null ?
                            row.get(colName) : row.get(alias)));
                }

                return key;
            }
        });
    }

    @Override
    protected List<Map<String, Object>> doCombine(Map<List<Tuple<String, Object>>, List<Map<String, Object>>> grouped,
                                                  Sql sql) {
        return MapGroupsCombiner.getInstance().combine(grouped, sql);
    }
}
