package common;

import cn.j.rhea.client.Rhea;
import cn.j.rhea.client.RheaPipeline;
import cn.j.rhea.client.RheaPool;
import com.google.common.base.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class RheaService {

    private final Logger logger = LoggerFactory.getLogger(RheaService.class);

    private RheaPool rheaPool;

    public RheaService(RheaPool rheaPool) {
        this.rheaPool = rheaPool;
    }

    public String hget(String key, String field) {
        String ret = null;
        Rhea rhea = null;
        try {
            rhea = rheaPool.getResource();
            ret = rhea.hget(key, field);
        } finally {
            if (rhea != null) {
                rheaPool.returnResource(rhea);
            }
        }
        return ret;
    }

    public byte[] hget(byte[] key, byte[] field) {
        byte[] ret = null;
        Rhea rhea = null;
        try {
            rhea = rheaPool.getResource();
            ret = rhea.hget(key, field);
        } finally {
            if (rhea != null) {
                rheaPool.returnResource(rhea);
            }
        }
        return ret;
    }

    public String get(String key) {
        String ret = null;
        Rhea rhea = null;
        try {
            rhea = rheaPool.getResource();
            ret = rhea.get(key);
        } finally {
            if (rhea != null) {
                rheaPool.returnResource(rhea);
            }
        }
        return ret;
    }

    public void set(byte[] key, byte[] value) {
        Rhea rhea = null;
        try {
            rhea = rheaPool.getResource();
            rhea.set(key, value);
        } finally {
            if (rhea != null) {
                rheaPool.returnResource(rhea);
            }
        }
    }

    public void set(String key, String value) {
        Rhea rhea = null;
        try {
            rhea = rheaPool.getResource();
            rhea.set(key, value);
        } finally {
            if (rhea != null) {
                rheaPool.returnResource(rhea);
            }
        }
    }

    public String lindex(String key, long index) {
        String ret = null;
        Rhea rhea = null;
        try {
            rhea = rheaPool.getResource();
            ret = rhea.lindex(key, index);
        } finally {
            if (rhea != null) {
                rheaPool.returnResource(rhea);
            }
        }
        return ret;
    }

    public long incr(String key) {
        long ret;
        Rhea rhea = null;
        try {
            rhea = rheaPool.getResource();
            ret = rhea.incr(key);
        } finally {
            if (rhea != null) {
                rheaPool.returnResource(rhea);
            }
        }
        return ret;
    }

    public long counterDecrease(String key) {
        long ret;
        Rhea rhea = null;
        try {
            rhea = rheaPool.getResource();
            ret = rhea.decr(key);
        } finally {
            if (rhea != null) {
                rheaPool.returnResource(rhea);
            }
        }
        return ret;
    }

    public void mset(byte[]... keysvalues) {
        Rhea rhea = null;
        try {
            rhea = rheaPool.getResource();
            RheaPipeline rheaPipeline = rhea.pipelined();
            for (int n = 0; n < keysvalues.length; n++) {
                byte[] key = keysvalues[n++];
                byte[] value = keysvalues[n];
                rheaPipeline.set(key, value);
            }
            rheaPipeline.syncAndReturnAll();
        } finally {
            if (rhea != null) {
                rheaPool.returnResource(rhea);
            }
        }
    }

    public Long hset(String key, String field, String value) {
        long ret;
        Rhea rhea = null;
        try {
            rhea = rheaPool.getResource();
            ret = rhea.hset(key, field, value);
        } finally {
            if (rhea != null) {
                rheaPool.returnResource(rhea);
            }
        }
        return ret;
    }

    public List<String> hmget(String key, String... fields) {
        List<String> ret = null;
        Rhea rhea = null;
        try {
            rhea = rheaPool.getResource();
            ret = rhea.hmget(key, fields);
        } finally {
            if (rhea != null) {
                rheaPool.returnResource(rhea);
            }
        }
        return ret;
    }

    public Map<String, String> hgetAll(String key) {
        Map<String, String> ret = null;
        Rhea rhea = null;
        try {
            rhea = rheaPool.getResource();
            ret = rhea.hgetAll(key);
        } finally {
            if (rhea != null) {
                rheaPool.returnResource(rhea);
            }
        }
        return ret;
    }

    public String hmset(String key, Map<String, String> hash) {
        String ret = null;
        Rhea rhea = null;
        try {
            rhea = rheaPool.getResource();
            ret = rhea.hmset(key, hash);
        } finally {
            if (rhea != null) {
                rheaPool.returnResource(rhea);
            }
        }
        return ret;
    }

    public Long hdel(String key, String[] fields) {
        Long ret = null;
        Rhea rhea = null;
        try {
            rhea = rheaPool.getResource();
            ret = rhea.hdel(key, fields);
        } finally {
            if (rhea != null) {
                rheaPool.returnResource(rhea);
            }
        }
        return ret;
    }

    public Long lpush(String key, String... strings) {
        long ret;
        Rhea rhea = null;
        try {
            rhea = rheaPool.getResource();
            ret = rhea.lpush(key, strings);
        } finally {
            if (rhea != null) {
                rheaPool.returnResource(rhea);
            }
        }
        return ret;
    }

    public Long zadd(String key, double score, String value) {
        Long ret;
        Rhea rhea = null;
        try {
            rhea = rheaPool.getResource();
            ret = rhea.zadd(key, score, value);
        } finally {
            if (rhea != null) {
                rheaPool.returnResource(rhea);
            }
        }
        return ret;
    }

    public Long zrank(String key, String value) {
        Long ret;
        Rhea rhea = null;
        try {
            rhea = rheaPool.getResource();
            ret = rhea.zrank(key, value);
        } finally {
            if (rhea != null) {
                rheaPool.returnResource(rhea);
            }
        }
        return ret;
    }

    public Set<String> zrange(String key, long start, long end) {
        Set<String> ret;
        Rhea rhea = null;
        try {
            rhea = rheaPool.getResource();
            ret = rhea.zrange(key, start, end);
        } finally {
            if (rhea != null) {
                rheaPool.returnResource(rhea);
            }
        }
        return ret;
    }

    public Set<String> zrangeByScore(String key, double min, double max) {
        Set<String> ret;
        Rhea rhea = null;
        try {
            rhea = rheaPool.getResource();
            ret = rhea.zrangeByScore(key, min, max);
        } finally {
            if (rhea != null) {
                rheaPool.returnResource(rhea);
            }
        }
        return ret;
    }

    public Long zremrangeByRank(String key, long start, long end) {
        Long ret;
        Rhea rhea = null;
        try {
            rhea = rheaPool.getResource();
            ret = rhea.zremrangeByRank(key, start, end);
        } finally {
            if (rhea != null) {
                rheaPool.returnResource(rhea);
            }
        }
        return ret;
    }

    public Long zremrangeByScore(String key, long start, long end) {
        Long ret;
        Rhea rhea = null;
        try {
            rhea = rheaPool.getResource();
            ret = rhea.zremrangeByScore(key, start, end);
        } finally {
            if (rhea != null) {
                rheaPool.returnResource(rhea);
            }
        }
        return ret;
    }


    public Long zcount(String key, long start, long end) {
        Long ret;
        Rhea rhea = null;
        try {
            rhea = rheaPool.getResource();
            ret = rhea.zcount(key, start, end);
        } finally {
            if (rhea != null) {
                rheaPool.returnResource(rhea);
            }
        }
        return ret;
    }


    public Long zrem(String key, String... member) {
        Long ret;
        Rhea rhea = null;
        try {
            rhea = rheaPool.getResource();
            ret = rhea.zrem(key, member);
        } finally {
            if (rhea != null) {
                rheaPool.returnResource(rhea);
            }
        }
        return ret;
    }

    public boolean exists(String key) {
        boolean ret = false;
        Rhea rhea = null;
        try {
            rhea = rheaPool.getResource();
            ret = rhea.exists(key);
        } finally {
            if (rhea != null) {
                rheaPool.returnResource(rhea);
            }
        }
        return ret;
    }

    public List<String> brpop(String key) {
        List<String> ret = null;
        Rhea rhea = null;
        try {
            rhea = rheaPool.getResource();
            ret = rhea.brpop(key);
        } finally {
            if (rhea != null) {
                rheaPool.returnResource(rhea);
            }
        }
        return ret;
    }

    public List<String> brpop(int timeout, String key) {
        List<String> ret = null;
        Rhea rhea = null;
        try {
            rhea = rheaPool.getResource();
            ret = rhea.brpop(timeout, key);
        } finally {
            if (rhea != null) {
                rheaPool.returnResource(rhea);
            }
        }
        return ret;
    }

    public Long setnx(String key, String value) {
        Long ret = null;
        Rhea rhea = null;
        try {
            rhea = rheaPool.getResource();
            ret = rhea.setnx(key, value);
        } finally {
            if (rhea != null) {
                rheaPool.returnResource(rhea);
            }
        }
        return ret;
    }

    public Long del(byte[] key) {
        Long ret = null;
        Rhea rhea = null;
        try {
            rhea = rheaPool.getResource();
            ret = rhea.del(key);
        } finally {
            if (rhea != null) {
                rheaPool.returnResource(rhea);
            }
        }
        return ret;
    }

    public Long del(String key) {
        Long ret = null;
        Rhea rhea = null;
        try {
            rhea = rheaPool.getResource();
            ret = rhea.del(key);
        } finally {
            if (rhea != null) {
                rheaPool.returnResource(rhea);
            }
        }
        return ret;
    }

    public Long srem(String key, String... member) {
        Long ret = null;
        Rhea rhea = null;
        try {
            rhea = rheaPool.getResource();
            ret = rhea.srem(key, member);
        } finally {
            if (rhea != null) {
                rheaPool.returnResource(rhea);
            }
        }
        return ret;
    }

    public Boolean sismember(String key, String member) {
        boolean ret = false;
        Rhea rhea = null;
        try {
            rhea = rheaPool.getResource();
            ret = rhea.sismember(key, member);

        } finally {
            if (rhea != null) {
                rheaPool.returnResource(rhea);
            }
        }
        return ret;
    }

    public Set<String> smembers(String key) {
        Set<String> ret = null;
        Rhea rhea = null;
        try {
            rhea = rheaPool.getResource();
            ret = rhea.smembers(key);
        } finally {
            if (rhea != null) {
                rheaPool.returnResource(rhea);
            }
        }
        return ret == null ? new HashSet<String>() : ret;
    }

    public Long sadd(String key, String... member) {
        Long ret = null;
        Rhea rhea = null;
        try {
            rhea = rheaPool.getResource();
            ret = rhea.sadd(key, member);
        } finally {
            if (rhea != null) {
                rheaPool.returnResource(rhea);
            }
        }
        return ret;
    }

    public String spop(String key) {
        String ret = null;
        Rhea rhea = null;
        try {
            rhea = rheaPool.getResource();
            ret = rhea.spop(key);
        } finally {
            if (rhea != null) {
                rheaPool.returnResource(rhea);
            }
        }
        return ret;
    }

    public Long expire(String key, int seconds) {
        Long ret = null;
        Rhea rhea = null;
        try {
            rhea = rheaPool.getResource();
            ret = rhea.expire(key, seconds);
        } finally {
            if (rhea != null) {
                rheaPool.returnResource(rhea);
            }
        }
        return ret;
    }

    public Set<String> keys(String pattern) {
        Set<String> ret = null;
        Rhea rhea = null;
        try {
            rhea = rheaPool.getResource();
            ret = rhea.keys(pattern);
        } finally {
            if (rhea != null) {
                rheaPool.returnResource(rhea);
            }
        }
        return ret;
    }

    public String getSet(String key, String value) {
        String ret = null;
        Rhea rhea = null;
        try {
            rhea = rheaPool.getResource();
            ret = rhea.getSet(key, value);
        } finally {
            if (rhea != null) {
                rheaPool.returnResource(rhea);
            }
        }
        return ret;
    }

    public <T> T usingRhea(Functions.Function1<Rhea, T> func) {
        Rhea rhea = null;
        T ret = null;
        try {
            rhea = rheaPool.getResource();
            ret = func.apply(rhea);
        } finally {
            if (rhea != null) {
                rheaPool.returnResource(rhea);
            }
        }
        return ret;
    }

    private String currentTimeMillis() {
        return String.valueOf(System.currentTimeMillis());
    }

    /**
     * 注意timeout需大于执行时间，否则其他线程会进入锁
     *
     * @param cacheKey        用于锁的key
     * @param timeoutInSecond 过期时间(秒)
     * @param func            执行过程
     */
    public <T> Optional<T> tryLock(String cacheKey, int timeoutInSecond, Functions.Function0<T> func) {
        long lockEnquired = 0l;
        try {
            if ((lockEnquired = setnx(cacheKey, String.valueOf(System.currentTimeMillis() + timeoutInSecond * 1000 + 1))) > 0) {
                return Optional.of(func.apply());
            } else if (get(cacheKey).compareTo(currentTimeMillis()) < 0
                    && getSet(cacheKey, String.valueOf(System.currentTimeMillis() + timeoutInSecond * 1000 + 1))
                    .compareTo(currentTimeMillis()) < 0) {
                lockEnquired = 1l;
                return Optional.of(func.apply());
            }
        } finally {
            if (lockEnquired > 0 && get(cacheKey).compareTo(currentTimeMillis()) >= 0) {
                del(cacheKey);
            }
        }
        return Optional.absent();
    }

    public Set<String> zrevrangebyscore(String key, double min, double max, int offset, int count) {
        Set<String> ret;
        Rhea rhea = null;
        try {
            rhea = rheaPool.getResource();
            ret = rhea.zrevrangeByScore(key, max, min, offset, count);
        } finally {
            if (rhea != null) {
                rheaPool.returnResource(rhea);
            }
        }
        return ret;
    }

    public String srandmember(final String key) {
        return usingRhea(new Functions.Function1<Rhea, String>() {
            @Override
            public String apply(Rhea rhea) {
                return rhea.srandmember(key);
            }
        });
    }
}
