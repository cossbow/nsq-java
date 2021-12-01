package com.cossbow.nsq.util;

import java.util.*;
import java.util.function.*;
import java.util.stream.Stream;

final
public class LambdaUtil {
    private LambdaUtil() {
    }

    //


    public static <T> Predicate<T> nonNull() {
        return Objects::nonNull;
    }

    public static <T> Predicate<T> isNull() {
        return Objects::isNull;
    }


    //

    public static ToLongFunction<Long> longVal() {
        return Long::longValue;
    }

    public static <T> ToLongFunction<T> longVal(Function<T, Long> mapper) {
        Objects.requireNonNull(mapper);
        return mapper::apply;
    }

    public static ToLongFunction<String> toLong() {
        return Long::parseLong;
    }

    public static <T> ToLongFunction<T> toLong(Function<T, String> mapper) {
        Objects.requireNonNull(mapper);
        return s -> Long.parseLong(mapper.apply(s));
    }

    public static LongFunction<String> longToString() {
        return Long::toString;
    }

    public static <T extends CharSequence> Predicate<T> contains(String s) {
        return s::contains;
    }

    public static <T, C extends Collection<T>> Predicate<T> contains(C s) {
        return s::contains;
    }

    //

    public static IntFunction<String[]> stringArray() {
        return String[]::new;
    }

    public static IntFunction<Long[]> longArray() {
        return Long[]::new;
    }

    public static IntFunction<Integer[]> integerArray() {
        return Integer[]::new;
    }

    //

    public static <T> Supplier<List<T>> listSupplier() {
        return ArrayList::new;
    }

    public static <T, A> Function<A, List<T>> listCreator(int size) {
        return a -> new ArrayList<>(size);
    }

    public static <T, A> Function<A, List<T>> listCreator() {
        return a -> new ArrayList<>();
    }

    public static <T> Supplier<Set<T>> setSupplier() {
        return HashSet::new;
    }

    public static <T, A> Function<A, Set<T>> setCreator(int size) {
        return a -> new HashSet<>(size);
    }

    public static <T, A> Function<A, Set<T>> setCreator() {
        return a -> new HashSet<>();
    }

    public static <T, C extends Collection<T>> Consumer<T> collectAdd(C c) {
        return c::add;
    }

    public static <T, C extends Collection<T>> BiConsumer<C, T> collectAdd() {
        return Collection::add;
    }

    public static <T, C extends Collection<T>> Consumer<C> collectAddAll(C c) {
        return c::addAll;
    }

    public static <T, C extends Collection<T>> BiConsumer<C, C> collectAddAll() {
        return Collection::addAll;
    }

    public static <T, C extends Collection<T>> Function<C, Stream<T>> collectionStream() {
        return Collection::stream;
    }

    public static <K, V> Supplier<Map<K, V>> mapSupplier() {
        return HashMap::new;
    }

    public static <K, V, A> Function<A, Map<K, V>> mapCreator(int size) {
        return a -> new HashMap<>(size);
    }

    public static <K, V, A> Function<A, Map<K, V>> mapCreator() {
        return a -> new HashMap<>();
    }

    public static <K, V> BiConsumer<K, V> mapPut(Map<K, V> m) {
        return m::put;
    }

    public static <K, V, M extends Map<K, V>> BiConsumer<M, M> mapPutAll() {
        return Map::putAll;
    }

    //

    public static <K, V, E extends Map.Entry<K, V>> Function<E, K> mapKey() {
        return Map.Entry::getKey;
    }

    public static <K, V, E extends Map.Entry<K, V>> Function<E, V> mapVal() {
        return Map.Entry::getValue;
    }

    //

    public static <T extends Enum<T>> ToIntFunction<T> ordinalFun() {
        return Enum::ordinal;
    }

    public static <T extends Enum<T>> IntFunction<T> ordinalIndex(Class<T> type) {
        return new OrdinalIndexTable<>(type);
    }

    static class OrdinalIndexTable<T extends Enum<T>> implements
            IntFunction<T>, Function<Integer, T> {
        private final Class<T> type;
        private final T[] table;

        OrdinalIndexTable(Class<T> type) {
            this.type = type;
            T[] values = type.getEnumConstants();
            if (null == values || values.length == 0) {
                throw new IllegalArgumentException("need non empty values");
            }
            this.table = values;
        }

        public T ordinalOf(int ordinal) {
            if (0 <= ordinal && ordinal < table.length) {
                T t = table[ordinal];
                if (null != t) {
                    return t;
                }
            }
            throw new IllegalArgumentException("illegal ordinal: "
                    + ordinal + " of " + type.getSimpleName());
        }

        @Override
        public T apply(int ordinal) {
            return ordinalOf(ordinal);
        }

        @Override
        public T apply(Integer ordinal) {
            return null == ordinal ? null : ordinalOf(ordinal);
        }

    }

    //

    public static <T, R> Function<T, R> just(R r) {
        return (t) -> r;
    }

    static final Function<?, Void> FUN_EMPTY = just(null);

    @SuppressWarnings("unchecked")
    public static <T> Function<T, Void> empty() {
        return (Function<T, Void>) FUN_EMPTY;
    }

    public static <T> Consumer<T> doNothing() {
        return t -> {
        };
    }

}
