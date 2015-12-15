package util;

import org.springframework.jdbc.core.JdbcTemplate;

import java.util.Arrays;
import java.util.Map;

/**
 * Created by guohang.bao on 15-7-21.
 */
public class DoubleWriteUpdater extends BaseUpdater {

    protected String tableName2;

    protected DoubleWriteJdbcTemplate doubleWriteJdbcTemplate;

    protected DoubleWriteUpdater(String tableName, String tableName2) {
        super(tableName);
        this.tableName2 = tableName2;
    }

    protected DoubleWriteUpdater(DoubleWriteUpdater from) {
        super(from);
        this.tableName2 = from.tableName2;
        this.doubleWriteJdbcTemplate = from.doubleWriteJdbcTemplate;
    }

    protected DoubleWriteUpdater(BaseUpdater from, String tableName2, DoubleWriteJdbcTemplate doubleWriteJdbcTemplate) {
        super(from);
        this.tableName2 = tableName2;
        this.doubleWriteJdbcTemplate = doubleWriteJdbcTemplate;
    }

    public static DoubleWriteUpdater tableName(String tableName, String tableName2) {
        return new DoubleWriteUpdater(tableName, tableName2);
    }

    DoubleWriteUpdater doubleWriteJdbcTemplate(DoubleWriteJdbcTemplate doubleWriteJdbcTemplate) {
        DoubleWriteUpdater ret = new DoubleWriteUpdater(this);
        ret.doubleWriteJdbcTemplate = doubleWriteJdbcTemplate;
        return ret;
    }

    @Override
    public DoubleWriteUpdater updateKV(String k, Object v) {
        return new DoubleWriteUpdater(super.updateKV(k, v),
                this.tableName2, this.doubleWriteJdbcTemplate);
    }

    @Override
    public DoubleWriteUpdater updateKV(String k1, Object v1, String k2, Object v2) {
        return new DoubleWriteUpdater(super.updateKV(k1, v1, k2, v2),
                this.tableName2, this.doubleWriteJdbcTemplate);
    }

    @Override
    public DoubleWriteUpdater updateKV(String k1, Object v1, String k2, Object v2, String k3, Object v3) {
        return new DoubleWriteUpdater(super.updateKV(k1, v1, k2, v2, k3, v3),
                this.tableName2, this.doubleWriteJdbcTemplate);
    }

    @Override
    public DoubleWriteUpdater updateKV(Map<String, Object> kv) {
        return new DoubleWriteUpdater(super.updateKV(kv), this.tableName2, this.doubleWriteJdbcTemplate);
    }

    @Override
    public DoubleWriteUpdater criteria(String criteria) {
        return new DoubleWriteUpdater(super.criteria(criteria), this.tableName2, this.doubleWriteJdbcTemplate);
    }

    protected String sql2() {
        return sqlByTableName(tableName2);
    }

    @Override
    public int update(final Object... args) {
        final boolean noArgs = BaseUtils.noArgs(args);

        return doubleWriteJdbcTemplate.doubleUpdate(new Functions.Function1<JdbcTemplate, Integer>() {

            public Integer apply(JdbcTemplate jdbcTemplate) {
                logger.debug("executing update: [" + sql() + "] " + Arrays.toString(args));

                return noArgs ? jdbcTemplate.update(sql()) : jdbcTemplate.update(sql(), args);
            }
        }, new Functions.Function1<JdbcTemplate, Integer>() {

            public Integer apply(JdbcTemplate jdbcTemplate) {
                logger.debug("executing update: [" + sql2() + "] " + Arrays.toString(args));

                return noArgs ? jdbcTemplate.update(sql2()) : jdbcTemplate.update(sql2(), args);
            }
        });
    }
}
