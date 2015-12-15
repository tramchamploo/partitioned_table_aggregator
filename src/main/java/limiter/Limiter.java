package limiter;

import bean.Sql;

import java.util.List;

/**
 * Created by guohang.bao on 15/10/28.
 */
public abstract class Limiter {

    protected Sql sql;

    protected Limiter(Sql sql) {
        this.sql = sql;
    }

    /**
     * trim leading before offset
     */
    public abstract <S> List<S> limit(List<S> preResult);

}
