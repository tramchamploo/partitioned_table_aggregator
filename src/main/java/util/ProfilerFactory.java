package util;

import org.slf4j.profiler.Profiler;

/**
 * Created by guohang.bao on 15-9-1.
 */
public class ProfilerFactory {

    public static Profiler createProfiler(String name) {
        return new Profiler(name);
    }
}
