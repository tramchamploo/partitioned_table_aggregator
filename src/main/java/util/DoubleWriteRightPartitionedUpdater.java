package util;

import aggregator.SubTableDataAggregator;
import org.springframework.jdbc.core.JdbcTemplate;

import java.util.Arrays;
import java.util.Map;

/**
 * Created by guohang.bao on 15-7-21.
 */
public class DoubleWriteRightPartitionedUpdater extends DoubleWriteUpdater {

    private SubTableDataAggregator dataAggregator;

    private DoubleWriteRightPartitionedUpdater(DoubleWriteRightPartitionedUpdater from) {
        super(from);
        this.dataAggregator = from.dataAggregator;
    }

    private DoubleWriteRightPartitionedUpdater(DoubleWriteUpdater from, SubTableDataAggregator dataAggregator,
                                               DoubleWriteJdbcTemplate doubleWriteJdbcTemplate) {
        super(from);
        this.dataAggregator = dataAggregator;
        this.doubleWriteJdbcTemplate = doubleWriteJdbcTemplate;
    }

    protected DoubleWriteRightPartitionedUpdater(String tableName, String tableName2) {
        super(tableName, tableName2);
    }

    public static DoubleWriteRightPartitionedUpdater tableName(String tableName, String tableName2) {
        return new DoubleWriteRightPartitionedUpdater(tableName, tableName2);
    }

    DoubleWriteRightPartitionedUpdater doubleWriteJdbcTemplate(DoubleWriteJdbcTemplate doubleWriteJdbcTemplate) {
        return new DoubleWriteRightPartitionedUpdater(this, this.dataAggregator, doubleWriteJdbcTemplate);
    }


    DoubleWriteRightPartitionedUpdater dataAggregator(SubTableDataAggregator dataAggregator) {
        return new DoubleWriteRightPartitionedUpdater(this, dataAggregator, this.doubleWriteJdbcTemplate);
    }

    @Override
    public DoubleWriteRightPartitionedUpdater updateKV(String k, Object v) {
        return new DoubleWriteRightPartitionedUpdater(super.updateKV(k, v),
                this.dataAggregator, this.doubleWriteJdbcTemplate);
    }

    @Override
    public DoubleWriteRightPartitionedUpdater updateKV(String k1, Object v1, String k2, Object v2) {
        return new DoubleWriteRightPartitionedUpdater(super.updateKV(k1, v1, k2, v2),
                this.dataAggregator, this.doubleWriteJdbcTemplate);
    }

    @Override
    public DoubleWriteRightPartitionedUpdater updateKV(String k1, Object v1, String k2, Object v2,
                                                       String k3, Object v3) {
        return new DoubleWriteRightPartitionedUpdater(super.updateKV(k1, v1, k2, v2, k3, v3),
                this.dataAggregator, this.doubleWriteJdbcTemplate);
    }

    @Override
    public DoubleWriteRightPartitionedUpdater updateKV(Map<String, Object> kv) {
        return new DoubleWriteRightPartitionedUpdater(super.updateKV(kv),
                this.dataAggregator, this.doubleWriteJdbcTemplate);
    }

    @Override
    public DoubleWriteRightPartitionedUpdater criteria(String criteria) {
        return new DoubleWriteRightPartitionedUpdater(super.criteria(criteria),
                this.dataAggregator, this.doubleWriteJdbcTemplate);
    }

    @Override
    public int update(final Object... args) {
        return doubleWriteJdbcTemplate.doubleUpdate(new Functions.Function1<JdbcTemplate, Integer>() {

            public Integer apply(JdbcTemplate jdbcTemplate) {
                logger.debug("executing update: [" + sql() + "] " + Arrays.toString(args));

                return BaseUtils.noArgs(args) ? jdbcTemplate.update(sql()) : jdbcTemplate.update(sql(), args);
            }
        }, new Functions.Function1<JdbcTemplate, Integer>() {

            public Integer apply(JdbcTemplate jdbcTemplate) {
                try {
                    return dataAggregator.update(args);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        });
    }
}
