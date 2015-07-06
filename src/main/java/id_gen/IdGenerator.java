package id_gen;

/**
 * Created by guohang.bao on 15-4-28.
 */
public interface IdGenerator {

    Long next();

    void forceSet(Long value);
}
