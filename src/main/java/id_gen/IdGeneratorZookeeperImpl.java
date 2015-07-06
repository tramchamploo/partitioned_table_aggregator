package id_gen;

import cn.j.rio.tools.PaperboySupport;
import cn.j.rio.tools.RheaService;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.atomic.AtomicValue;
import org.apache.curator.framework.recipes.atomic.DistributedAtomicLong;
import org.apache.curator.framework.recipes.atomic.PromotedToLock;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.curator.retry.BoundedExponentialBackoffRetry;
import org.apache.curator.retry.RetryUntilElapsed;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import static java.util.concurrent.TimeUnit.MINUTES;

/**
 * Created by guohang.bao on 15-4-28.
 */
public abstract class IdGeneratorZookeeperImpl implements DbBasedIdGenerator, PaperboySupport {

    private CuratorFramework client;

    private ZookeeperIdGeneratorConfig config;

    private DbBasedIdGenerator anotherGenerator;

    private RheaService rheaService;

    private final ScheduledExecutorService scheduler =
            Executors.newSingleThreadScheduledExecutor();

    private Logger logger = LoggerFactory.getLogger(IdGeneratorZookeeperImpl.class);


    public IdGeneratorZookeeperImpl(ZookeeperIdGeneratorConfig config, String connection,
                                    DbBasedIdGenerator another,
                                    RheaService rheaService) {
        this.config = config;
        this.anotherGenerator = another;
        this.rheaService = rheaService;

        client = CuratorFrameworkFactory.newClient(connection, new RetryUntilElapsed(20 * 1000, 1000));
        client.getConnectionStateListenable().addListener(new ConnectionStateListener() {
            @Override
            public void stateChanged(CuratorFramework curatorFramework, ConnectionState connectionState) {
                switch (connectionState) {
                    case LOST:
                        onConnectionLost();
                        break;
                    case RECONNECTED:
                        recover();
                        break;
                    case CONNECTED:
                        onConnect();
                        break;
                    case SUSPENDED:
                        onSuspended();
                        break;
                }
            }
        });
        client.start();
    }

    private void onConnectionLost() {
        if (!ConnectionState.LOST.name().equals(rheaService.get(config.getZookeeperStatusRedisKey())) &&
                !ConnectionState.LOST.name().equals(rheaService.getSet(config.getZookeeperStatusRedisKey(), ConnectionState.LOST.name()))) {
            anotherGenerator.syncFromDB();
            String errMsg = "ZOOKEEPER CONNECTION LOST, SWITCH TO REDIS";
            logger.error(errMsg);
            paperBoy().error(errMsg, config.getPaperboyLogCategory());
        }
    }

    private void onConnect() {
        long value = 0L;
        try {
            DistributedAtomicLong atomicLong = postIdAtomicLong();
            if (!ConnectionState.CONNECTED.name().equals(rheaService.get(config.getZookeeperStatusRedisKey())) &&
                    !ConnectionState.CONNECTED.name()
                            .equals(rheaService.getSet(config.getZookeeperStatusRedisKey(), ConnectionState.CONNECTED.name()))) {
                doRecover();
            } else if (atomicLong.initialize(0L)) {
                value = maxIdFromDB() + SYNC_OFFSET;
                atomicLong.compareAndSet(0L, value);
            }
        } catch (Exception e) {
            logger.error("zookeeper id init error, value: " + value, e);
        }
    }

    private void onSuspended() {
        if (!rheaService.get(config.getZookeeperStatusRedisKey()).equals(ConnectionState.SUSPENDED.name()) &&
                !rheaService.getSet(config.getZookeeperStatusRedisKey(), ConnectionState.SUSPENDED.name())
                        .equals(ConnectionState.SUSPENDED.name())) {
            String errMsg = "ZOOKEEPER CONNECTION SUSPENDED";
            logger.warn(errMsg);
            paperBoy().error(errMsg, config.getPaperboyLogCategory());
        }
    }

    private void doRecover() {
        anotherGenerator.recover();
        syncFromDB();
        logAndNotifyRecoverMessage();
    }

    private void logAndNotifyRecoverMessage() {
        String msg = "ZOOKEEPER ID GENERATOR RECOVER SUCCESSFULLY!";
        paperBoy().error(msg, config.getPaperboyLogCategory());
        logger.info(msg);
    }

    @Override
    public void recover() {
        scheduler.schedule(new Runnable() {
            @Override
            public void run() {
                if (!ConnectionState.CONNECTED.name().equals(rheaService.get(config.getZookeeperStatusRedisKey())) &&
                        !ConnectionState.CONNECTED.name()
                                .equals(rheaService.getSet(config.getZookeeperStatusRedisKey(), ConnectionState.CONNECTED.name()))) {
                    doRecover();
                } else {
                    logger.error("ZOOKEEPER ID GENERATOR ALREADY RECOVERED!");
                }
            }
        }, 30, MINUTES);
    }

    @Override
    public Long next() {
        if (ConnectionState.LOST.name().equals(rheaService.get(config.getZookeeperStatusRedisKey()))) {
            return anotherGenerator.next();
        } else if (ConnectionState.SUSPENDED.name().equals(rheaService.get(config.getZookeeperStatusRedisKey()))) {
            throw new RuntimeException("zookeeper connection suspended!");
        } else {
            try {
                return incr(0, config.getnRetries());
            } catch (ZookeeperException e) {
                onConnectionLost();
                return anotherGenerator.next();
            }
        }
    }

    private Long incr(int times, int limit) throws ZookeeperException {
        if (ConnectionState.LOST.name().equals(rheaService.get(config.getZookeeperStatusRedisKey()))) {
            throw new RuntimeException("zookeeper already down, stop retry.");
        } else if (times >= limit) {
            throw new ZookeeperException("exceed retry limit!");
        } else {
            DistributedAtomicLong atomicLong = postIdAtomicLong();
            try {
                AtomicValue<Long> value = atomicLong.increment();
                if (value.succeeded()) return value.postValue();
                else return incr(times + 1, limit);
            } catch (Exception e) {
                return incr(times + 1, limit);
            }
        }
    }

    private DistributedAtomicLong postIdAtomicLong() {
        return new DistributedAtomicLong(client, config.getKeyPath(), new BoundedExponentialBackoffRetry(50, 5000, 3),
                PromotedToLock.builder().lockPath(config.getPromotedLockPath()).
                        retryPolicy(new BoundedExponentialBackoffRetry(50, 5000, 3)).build());
    }

    @Override
    public Long syncFromDB() {
        Long ret = maxIdFromDB();
        try {
            long newId = ret + SYNC_OFFSET;
            postIdAtomicLong().forceSet(newId);
            logger.info("id gen synced with db, new id: {}", newId);
        } catch (Exception e) {
            logger.error("syncFromDB", e);
        }
        return ret;
    }

    @Override
    public void forceSet(Long value) {
        try {
            postIdAtomicLong().forceSet(value);
        } catch (Exception e) {
            logger.error("forceSet", e);
        }
    }

}

class ZookeeperException extends Exception {
    public ZookeeperException(String msg) {
        super(msg);
    }
}