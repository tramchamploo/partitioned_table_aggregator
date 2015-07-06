package id_gen;

import cn.j.rio.tools.RheaService;
import org.springframework.jdbc.core.JdbcTemplate;

/**
 * Created by guohang.bao on 15-4-28.
 */
public class PostIdGeneratorRedisImpl implements DbBasedIdGenerator {

    private IdGeneratorRedisImpl idGeneratorRedisImpl;

    public PostIdGeneratorRedisImpl(final JdbcTemplate jdbcTemplate, RheaService rheaService) {
        IdGeneratorConfig config = IdGeneratorConfig.builder().keyPath("medusa.id.current").build();
        this.idGeneratorRedisImpl = new IdGeneratorRedisImpl(config, rheaService) {
            @Override
            public Long maxIdFromDB() {
                return jdbcTemplate.queryForObject("SELECT max(id) FROM forum_post", Long.class);
            }
        };
    }

    @Override
    public Long syncFromDB() {
        return idGeneratorRedisImpl.syncFromDB();
    }

    @Override
    public void recover() {
        idGeneratorRedisImpl.recover();
    }

    @Override
    public Long maxIdFromDB() {
        return idGeneratorRedisImpl.maxIdFromDB();
    }

    @Override
    public Long next() {
        return idGeneratorRedisImpl.next();
    }

    @Override
    public void forceSet(Long value) {
        idGeneratorRedisImpl.forceSet(value);
    }
}
