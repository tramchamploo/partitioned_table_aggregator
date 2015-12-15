package limiter;

import bean.Limit;
import bean.Sql;
import com.google.common.collect.Lists;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by guohang.bao on 15/10/28.
 */
public class DefaultLimiter extends Limiter {

    public DefaultLimiter(Sql sql) {
        super(sql);
    }

    /**
     * trim leading before offset
     */
    public <S> List<S> limit(List<S> preResult) {
        Limit limit = sql.limit();
        if (limit.isNone()) {
            return preResult;
        }

        int start = Math.min(limit.getOffset(), preResult.size());
        int end = Math.min(limit.count(), preResult.size());

        if (preResult instanceof ArrayList) {
            return Lists.newArrayList(preResult.subList(start, end));
        }
        return Lists.newLinkedList(preResult.subList(start, end));
    }
}
