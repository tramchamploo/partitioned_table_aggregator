package id_gen;

import common.RheaService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by guohang.bao on 15-4-28.
 */
public abstract class IdGeneratorRedisImpl implements DbBasedIdGenerator {

    private RheaService rheaService;

    private IdGeneratorConfig config;

    private Logger logger = LoggerFactory.getLogger(IdGeneratorRedisImpl.class.getSimpleName());

    public IdGeneratorRedisImpl(IdGeneratorConfig config, RheaService rheaService) {
        this.config = config;
        this.rheaService = rheaService;
    }

    public Long syncFromDB() {
        Long newId = maxIdFromDB() + SYNC_OFFSET;
        logger.info("new id set to redis, value: {}", newId);
        rheaService.setnx(config.getKeyPath(), String.valueOf(newId));
        return newId;
    }

    @Override
    public void forceSet(Long value) {
        rheaService.set(config.getKeyPath(), value.toString());
    }

    @Override
    public Long next() {
        Long postValue = rheaService.incr(config.getKeyPath());
        if (postValue == 1) {
            rheaService.del(config.getKeyPath());
            throw new RuntimeException("counter should not be incr if it doesn't exist!");
        }
        return postValue;
    }

    @Override
    public void recover() {
        rheaService.del(config.getKeyPath());
    }

}
