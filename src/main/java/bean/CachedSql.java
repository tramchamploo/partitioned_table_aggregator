package bean;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import util.tuple.Tuple;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * Created by guohang.bao on 15-6-25.
 */
public class CachedSql extends Sql {

    private Sql proxied;

    private Logger logger = LoggerFactory.getLogger(CachedSql.class);

    private static final LoadingCache<Sql, String[]> sqlCache = CacheBuilder.newBuilder().maximumSize(2000)
            .expireAfterWrite(1, TimeUnit.DAYS).build(new CacheLoader<Sql, String[]>() {
                @Override
                public String[] load(Sql key) throws Exception {
                    return key.subSqls();
                }
            });


    public CachedSql(String sql,
                     Object[] args,
                     LinkedHashMap<String, SelectExpressionItem> fields,
                     Map<Function, String> functions,
                     List<Column> groupBys,
                     Tuple<String, Boolean> orderBy,
                     Limit limit) {
        super(sql, args, fields, functions, groupBys, orderBy, limit);
        proxied = new Sql(sql, args, fields, functions, groupBys, orderBy, limit);
    }

    @Override
    public String[] subSqls() {
        try {
            return sqlCache.get(proxied);
        } catch (ExecutionException e) {
            logger.error("sub sqls from cache error:", e);
        }
        return proxied.subSqls();
    }

    @Override
    public Sql cached() {
        throw new UnsupportedOperationException("double cached!");
    }
}
