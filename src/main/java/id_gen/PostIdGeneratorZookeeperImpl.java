package id_gen;

import cn.j.rio.tools.RheaService;
import com.langtaojin.guang.coreutil.paperboy.PaperBoyLogger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;

/**
 * Created by guohang.bao on 15-4-28.
 */
public class PostIdGeneratorZookeeperImpl implements DbBasedIdGenerator {

    private IdGeneratorZookeeperImpl idGeneratorZookeeperImpl;

    private Logger logger = LoggerFactory.getLogger(PostIdGeneratorZookeeperImpl.class);


    public PostIdGeneratorZookeeperImpl(String connection,
                                        DbBasedIdGenerator another, final JdbcTemplate jdbcTemplate,
                                        RheaService rheaService, final PaperBoyLogger paperBoyLogger) {
        ZookeeperIdGeneratorConfig config = ZookeeperIdGeneratorConfig.builder()
                .zookeeperStatusRedisKey("medusa.zookeeper.status")
                .promotedLockPath("/medusa/post/id_promoted")
                .paperboyLogCategory("medusa.post_id_generator")
                .keyPath("/medusa/post/id")
                .nRetries(3)
                .build();
        logger.info("init with config: {}", config);
        this.idGeneratorZookeeperImpl = new IdGeneratorZookeeperImpl(config, connection,
                another, rheaService) {
            @Override
            public PaperBoyLogger paperBoy() {
                return paperBoyLogger;
            }

            @Override
            public Long maxIdFromDB() {
                return jdbcTemplate.queryForObject("SELECT max(id) FROM forum_post", Long.class);
            }
        };
    }

    @Override
    public Long syncFromDB() {
        return idGeneratorZookeeperImpl.syncFromDB();
    }

    @Override
    public void recover() {
        idGeneratorZookeeperImpl.recover();
    }

    @Override
    public Long maxIdFromDB() {
        return idGeneratorZookeeperImpl.maxIdFromDB();
    }

    @Override
    public Long next() {
        return idGeneratorZookeeperImpl.next();
    }

    @Override
    public void forceSet(Long value) {
        idGeneratorZookeeperImpl.forceSet(value);
    }
}

