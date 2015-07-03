package partition;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;

import java.util.Arrays;

/**
 * Created by guohang.bao on 15-6-25.
 */
public class Sql {

    private static final String PLACEHOLDER = "$";

    private static final int N_MAX_SUB_TABLES = 8;

    private int nSubTables = 0;

    protected String sql;

    protected Limit limit;

    protected SubTable[] subTables;

    public Sql(String sql, Limit limit, SubTable... subTables) {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(sql));
        Preconditions.checkArgument(subTables != null && subTables.length > 0);
        for (int i = 1; i < subTables.length; i++) {
            Preconditions.checkArgument(subTables[0].numOfTables() == subTables[i].numOfTables());
        }
        this.sql = sql;
        this.subTables = subTables;
        initNSubTables();
        this.limit = limit;
    }

    public Sql(String sql, SubTable... subTables) {
        this(sql, Limit.none(), subTables);
    }

    private void initNSubTables() {
        int idx = 0;
        for (int i = 1; i <= N_MAX_SUB_TABLES; i++) {
            if ((idx = sql.indexOf(PLACEHOLDER + i, idx)) > -1) nSubTables++;
        }
        Preconditions.checkArgument(nSubTables == subTables.length);
    }

    public String[] subSqls() {
        int numOfResult = subTables[0].numOfTables();
        String[] ret = new String[numOfResult];
        for (int i = 0; i < numOfResult; i++) {
            String sqlCopy = sql;
            for (int j = 1; j <= nSubTables; j++) {
                sqlCopy = sqlCopy.replace(PLACEHOLDER + j, subTables[j - 1].getTableNames()[i]);
            }
            ret[i] = appendLimit(sqlCopy);
        }
        return ret;
    }

    Limit limit() {
        return limit;
    }

    private String appendLimit(String src) {
        if (!src.toLowerCase().contains(" limit ")) {
            if (limit() instanceof Limit.None) {
                return src;
            } else if (src.endsWith(";")) {
                return src.substring(0, src.length() - 1) + " LIMIT " + limit().count() + ";";
            } else {
                return src + " LIMIT " + limit().count();
            }
        }
        return src;
    }

    public Sql cached() {
        return new CachedSql(sql, limit, subTables);
    }

    public String toString() {
        return Joiner.on("\n").join(subSqls());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Sql sql1 = (Sql) o;

        if (sql != null ? !sql.equals(sql1.sql) : sql1.sql != null) return false;
        if (limit != null ? !limit.equals(sql1.limit) : sql1.limit != null) return false;
        // Probably incorrect - comparing Object[] arrays with Arrays.equals
        return Arrays.equals(subTables, sql1.subTables);

    }

    @Override
    public int hashCode() {
        int result = sql != null ? sql.hashCode() : 0;
        result = 31 * result + (limit != null ? limit.hashCode() : 0);
        result = 31 * result + (subTables != null ? Arrays.hashCode(subTables) : 0);
        return result;
    }
}
