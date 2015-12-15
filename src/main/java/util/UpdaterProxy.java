package util;

import aggregator.ParallelizedNormalSortSubTableDataAggregatorFactory;
import aggregator.SubTableDataAggregatorFactory;
import bean.Sql;

import java.util.Map;

/**
 * Created by guohang.bao on 15-7-21.
 */
public class UpdaterProxy extends DoubleWriteRightPartitionedUpdater {

    private DoubleWriteJdbcTemplate doubleWriteJdbcTemplate;

    private DoubleWriteRightPartitionedUpdater updater;

    private SubTableDataAggregatorFactory dataAggregatorFactory =
            new ParallelizedNormalSortSubTableDataAggregatorFactory();


    public UpdaterProxy(DoubleWriteJdbcTemplate doubleWriteJdbcTemplate, String tableName) {
        super(tableName, tableName);
        this.doubleWriteJdbcTemplate = doubleWriteJdbcTemplate;
        updater = DoubleWriteRightPartitionedUpdater.tableName(tableName, tableName)
                .doubleWriteJdbcTemplate(doubleWriteJdbcTemplate);
    }

    public int update(Object... args) {
        return updater.dataAggregator(dataAggregatorFactory.newInstance(doubleWriteJdbcTemplate.getJdbcTemplateRight(),
                new Sql(updater.sql2(), args),
                Object.class))
                .update(args);
    }

    public UpdaterProxy updateKV(String k, Object v) {
        updater = updater.updateKV(k, v);
        return this;
    }

    public UpdaterProxy updateKV(String k1, Object v1,
                                 String k2, Object v2) {
        updater = updater.updateKV(k1, v1, k2, v2);
        return this;
    }

    public UpdaterProxy updateKV(String k1, Object v1,
                                 String k2, Object v2,
                                 String k3, Object v3) {
        updater = updater.updateKV(k1, v1, k2, v2, k3, v3);
        return this;
    }

    public UpdaterProxy updateKV(Map<String, Object> kv) {
        updater = updater.updateKV(kv);
        return this;
    }

    public UpdaterProxy criteria(String criteria) {
        updater = updater.criteria(criteria);
        return this;
    }
}