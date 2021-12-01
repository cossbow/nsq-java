package com.cossbow.nsq.util;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collector;
import java.util.stream.Stream;

final
public class FutureUtil {
    private FutureUtil() {
    }


    //

    /**
     * @see #zip(CompletableFuture, CompletableFuture, CompletableFuture, CompletableFuture, CompletableFuture, CompletableFuture)
     */
    public static <T1, T2>
    CompletableFuture<TupleN<T1, T2, Void, Void, Void, Void>> zip(
            CompletableFuture<T1> f1,
            CompletableFuture<T2> f2) {
        return CompletableFuture.allOf(f1, f2)
                .thenApply(joinAll(f1, f2, null, null, null, null));
    }

    /**
     * @see #zip(CompletableFuture, CompletableFuture, CompletableFuture, CompletableFuture, CompletableFuture, CompletableFuture)
     */
    public static <T1, T2, T3>
    CompletableFuture<TupleN<T1, T2, T3, Void, Void, Void>> zip(
            CompletableFuture<T1> f1,
            CompletableFuture<T2> f2,
            CompletableFuture<T3> f3) {
        return CompletableFuture.allOf(f1, f2, f3)
                .thenApply(joinAll(f1, f2, f3, null, null, null));
    }

    /**
     * @see #zip(CompletableFuture, CompletableFuture, CompletableFuture, CompletableFuture, CompletableFuture, CompletableFuture)
     */
    public static <T1, T2, T3, T4>
    CompletableFuture<TupleN<T1, T2, T3, T4, Void, Void>> zip(
            CompletableFuture<T1> f1,
            CompletableFuture<T2> f2,
            CompletableFuture<T3> f3,
            CompletableFuture<T4> f4) {
        return CompletableFuture.allOf(f1, f2, f3, f4)
                .thenApply(joinAll(f1, f2, f3, f4, null, null));
    }

    /**
     * @see #zip(CompletableFuture, CompletableFuture, CompletableFuture, CompletableFuture, CompletableFuture, CompletableFuture)
     */
    public static <T1, T2, T3, T4, T5>
    CompletableFuture<TupleN<T1, T2, T3, T4, T5, Void>> zip(
            CompletableFuture<T1> f1,
            CompletableFuture<T2> f2,
            CompletableFuture<T3> f3,
            CompletableFuture<T4> f4,
            CompletableFuture<T5> f5) {
        return CompletableFuture.allOf(f1, f2, f3, f4, f5)
                .thenApply(joinAll(f1, f2, f3, f4, f5, null));
    }

    /**
     * waiting nullable result futures,
     * result is TupleN with nullable value
     */
    public static <T1, T2, T3, T4, T5, T6>
    CompletableFuture<TupleN<T1, T2, T3, T4, T5, T6>> zip(
            CompletableFuture<T1> f1,
            CompletableFuture<T2> f2,
            CompletableFuture<T3> f3,
            CompletableFuture<T4> f4,
            CompletableFuture<T5> f5,
            CompletableFuture<T6> f6) {
        return CompletableFuture.allOf(f1, f2, f3, f4, f5, f6)
                .thenApply(joinAll(f1, f2, f3, f4, f5, f6));
    }

    static <S, T1, T2, T3, T4, T5, T6> Function<S, TupleN<T1, T2, T3, T4, T5, T6>>
    joinAll(CompletableFuture<T1> f1,
            CompletableFuture<T2> f2,
            CompletableFuture<T3> f3,
            CompletableFuture<T4> f4,
            CompletableFuture<T5> f5,
            CompletableFuture<T6> f6) {

        return s -> new TupleN<>(
                joinOrNull(f1),
                joinOrNull(f2),
                joinOrNull(f3),
                joinOrNull(f4),
                joinOrNull(f5),
                joinOrNull(f6)
        );
    }

    static <T> T joinOrNull(CompletableFuture<T> future) {
        return null == future ? null : future.join();
    }

    public static class TupleN<T1, T2, T3, T4, T5, T6> {
        private final T1 t1;
        private final T2 t2;
        private final T3 t3;
        private final T4 t4;
        private final T5 t5;
        private final T6 t6;

        public TupleN(T1 t1, T2 t2, T3 t3, T4 t4, T5 t5, T6 t6) {
            this.t1 = t1;
            this.t2 = t2;
            this.t3 = t3;
            this.t4 = t4;
            this.t5 = t5;
            this.t6 = t6;
        }

        public T1 getT1() {
            return t1;
        }

        public T2 getT2() {
            return t2;
        }

        public T3 getT3() {
            return t3;
        }

        public T4 getT4() {
            return t4;
        }

        public T5 getT5() {
            return t5;
        }

        public T6 getT6() {
            return t6;
        }
    }


    //

    public
    static final CompletableFuture<?>[] EMPTY = new CompletableFuture[0];

    public
    static <S, F extends CompletableFuture<S>, C extends Collection<F>>
    CompletableFuture<Void> allOf(C futures) {
        if (futures.isEmpty()) {
            return CompletableFuture.completedFuture(null);
        }
        if (1 == futures.size()) {
            var f = futures.iterator().next();
            return f.thenAccept(LambdaUtil.doNothing());
        }
        return CompletableFuture.allOf(futures.toArray(EMPTY));
    }

    public
    static <S, F extends CompletableFuture<S>, C extends Collection<F>, T>
    CompletableFuture<T> allOf(C futures, Function<Void, T> supplier) {
        return allOf(futures).thenApply(supplier);
    }


    public
    static <T, F extends CompletableFuture<T>, C extends Collection<F>, R, A>
    CompletableFuture<A> collect(C src, Collector<T, R, A> collector) {
        var futures = new ArrayList<CompletableFuture<Void>>(src.size());
        var builder = Stream.<T>builder();
        for (var s : src) {
            futures.add(s.thenAccept((t) -> {
                synchronized (builder) {
                    builder.add(t);
                }
            }));
        }
        return allOf(futures, v -> {
            synchronized (builder) {
                return builder.build().collect(collector);
            }
        });
    }


    //

    public static <T, F extends CompletableFuture<T>>
    void transform(CompletionStage<T> stage, F f) {
        stage.whenComplete((t, e) -> {
            if (null == e) f.complete(t);
            else f.completeExceptionally(e);
        });
    }


    //

    /**
     * @param source     start future source
     * @param retryTimes retry times
     * @param retryDelay retry delay time, 0-retry immediately
     */
    public static <T> RetryFuture<T> retry(
            Supplier<CompletableFuture<T>> source,
            int retryTimes, Duration retryDelay,
            Predicate<? super Throwable> errorFilter) {
        var task = new RetryFuture<>(source, retryTimes, retryDelay,
                errorFilter);
        task.run();
        return task;
    }

    public static <T> RetryFuture<T> retry(
            Supplier<CompletableFuture<T>> call,
            int retryTimes, Duration retryDelay) {
        return retry(call, retryTimes, retryDelay, null);
    }

    public static <T> RetryFuture<T> retry(
            Supplier<CompletableFuture<T>> call,
            int retryTimes) {
        return retry(call, retryTimes, null, null);
    }

}
