package util;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMap;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;

import java.util.Date;
import java.util.Map;

/**
 * Created by guohang.bao on 15-7-21.
 */
public class BaseUpdater {

    protected String tableName;

    protected Map<String, Object> updateKV;

    protected String criteria;

    protected JdbcTemplate jdbcTemplate;

    protected static final Logger logger = LoggerFactory.getLogger(BaseUpdater.class);


    protected BaseUpdater(BaseUpdater from) {
        this.tableName = from.tableName;
        this.updateKV = from.updateKV;
        this.criteria = from.criteria;
        this.jdbcTemplate = from.jdbcTemplate;
    }

    protected BaseUpdater(String tableName) {
        this.tableName = tableName;
    }

    public static BaseUpdater tableName(String tableName) {
        return new BaseUpdater(tableName);
    }

    public BaseUpdater jdbcTemplate(JdbcTemplate jdbcTemplate) {
        BaseUpdater ret = new BaseUpdater(this);
        ret.jdbcTemplate = jdbcTemplate;
        return ret;
    }

    public BaseUpdater updateKV(String k, Object v) {
        BaseUpdater ret = new BaseUpdater(this);
        ret.updateKV = ImmutableMap.of(k, v);
        return ret;
    }

    public BaseUpdater updateKV(String k1, Object v1, String k2, Object v2) {
        BaseUpdater ret = new BaseUpdater(this);
        ret.updateKV = ImmutableMap.of(k1, v1, k2, v2);
        return ret;
    }

    public BaseUpdater updateKV(String k1, Object v1, String k2, Object v2, String k3, Object v3) {
        BaseUpdater ret = new BaseUpdater(this);
        ret.updateKV = ImmutableMap.of(k1, v1, k2, v2, k3, v3);
        return ret;
    }

    public BaseUpdater updateKV(Map<String, Object> kv) {
        BaseUpdater ret = new BaseUpdater(this);
        ret.updateKV = kv;
        return ret;
    }

    public BaseUpdater criteria(String criteria) {
        BaseUpdater ret = new BaseUpdater(this);
        ret.criteria = criteria;
        return ret;
    }

    public int update(Object... args) {
        return BaseUtils.noArgs(args) ? jdbcTemplate.update(sql()) : jdbcTemplate.update(sql(), args);
    }

    protected String sqlByTableName(String tableName) {
        return Joiner.on(" ").join("UPDATE", tableName, "SET", setPart(), "WHERE", criteria);
    }

    protected String sql() {
        return sqlByTableName(tableName);
    }

    protected String setPart() {
        StringBuilder buffer = new StringBuilder();
        int idx = 0;

        for (Map.Entry<String, Object> entry : updateKV.entrySet()) {
            if (idx > 0) buffer.append(", ");
            buffer.append(entry.getKey()).append(" = ");

            Object value = entry.getValue();

            if (value instanceof Number) {
                buffer.append(value);
            } else if (value instanceof Date) {
                DateTime d = new DateTime(((Date) value).getTime());
                buffer.append("'").append(d.toString("yyyy-MM-dd HH:mm:ss")).append("'");
            } else if (value instanceof Boolean) {
                buffer.append((Boolean) value ? "1": "0");
            } else {
                buffer.append("'").append(value).append("'");
            }

            idx++;
        }
        return buffer.toString();
    }
}
