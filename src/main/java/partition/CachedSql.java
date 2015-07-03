package partition;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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


    public CachedSql(String sql, Limit limit, SubTable... subTables) {
        super(sql, limit, subTables);
        proxied = new Sql(sql, limit, subTables);
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
