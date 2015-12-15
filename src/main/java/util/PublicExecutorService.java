package util;

import com.google.common.util.concurrent.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * Created by guohang.bao on 14-8-1.
 */
public class PublicExecutorService {

    private Logger logger = LoggerFactory.getLogger(PublicExecutorService.class);

    private ListeningExecutorService executorService;

    public PublicExecutorService(int nThreads) {
        executorService = MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(nThreads));
    }

    public PublicExecutorService() {
        executorService = MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(300));
    }

    public <T> ListenableFuture<T> submit(Callable<T> callable, FutureCallback<T> callback) {
        ListenableFuture<T> future = executorService.submit(callable);
        logger.debug("new task submitted: {}", callable);
        Futures.addCallback(future, callback);
        return future;
    }

    public <T> ListenableFuture<T> submit(Callable<T> callable) {
        ListenableFuture<T> future = executorService.submit(callable);
        logger.debug("new task submitted: {}", callable);
        return future;
    }

    public <T> T invokeAny(Collection<? extends Callable<T>> tasks)
            throws ExecutionException, InterruptedException {
        T ret = executorService.invokeAny(tasks);
        logger.debug("new tasks submitted: {}", tasks);
        return ret;
    }

    public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks)
            throws ExecutionException, InterruptedException {
        List<Future<T>> futures = executorService.invokeAll(tasks);
        logger.debug("new tasks submitted: {}", tasks);
        return futures;
    }

}
