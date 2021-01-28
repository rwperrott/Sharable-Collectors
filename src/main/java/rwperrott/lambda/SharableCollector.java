package rwperrott.lambda;

import java.util.*;
import java.util.function.*;
import java.util.stream.Collector;

import static java.lang.String.format;

/**
 * Handles both pre and post mapping.
 * <p>
 * I did attempt to validate by Class<T>, however that didn't work for objects with erased types like Tuple*!
 *
 * @param <T>  source value type
 * @param <U>  accumulated value type
 * @param <A>  accumulator type, which is probably the same as R
 * @param <R>  intermediate result type
 * @param <RR> final result type
 */
@SuppressWarnings("unchecked")
public final class SharableCollector<T, U, A, R, RR> implements Collector<T, A, RR> {
    // Stored for sharing validation
    private final Function<T, U> mapper;
    // Stored for sharing validation
    private final Collector<U, ?, R> collectorR;
    // Stored for sharing check and to create finisher
    private final UnaryOperator<R> andThenR;
    // Used to create finisher
    private final Function<R, RR> andThenRR;
    //
    // Actual Collector functions
    private final Supplier<A> supplier;
    private final BiConsumer<A, T> accumulator;
    private final BinaryOperator<A> combiner;
    private Function<A, RR> finisher;
    //
    // Intermediate result
    private R r;
    private int shareCount = -1;
    // If true assumes that sharers of r won't make destructive changes
    // e.g. just sorting is OK.
    private boolean sameAndThenR = true;

    @SuppressWarnings("unchecked")
    private SharableCollector(final Function<T, U> mapper,
                              final Collector<U, ?, R> collectorR,
                              final UnaryOperator<R> andThenR,
                              final Function<R, RR> andThenRR) {
        this.mapper = Objects.requireNonNull(mapper, "mapper");
        this.collectorR = Objects.requireNonNull(collectorR, "collectorR");
        this.supplier = (Supplier<A>) collectorR.supplier();
        final BiConsumer<A, U> accumulator = (BiConsumer<A, U>) collectorR.accumulator();
        this.accumulator = isIdentityFunction(mapper)
                           ? (BiConsumer<A, T>) accumulator
                           : (r, t) -> accumulator.accept(r, mapper.apply(t)); // Based upon Collectors.mapping method
        this.combiner = (BinaryOperator<A>) collectorR.combiner();
        this.andThenR = Objects.requireNonNull(andThenR, "andThenR");
        this.andThenRR = Objects.requireNonNull(andThenRR, "andThenRR");
    }

    /**
     * Has to check both Function.identity() and UnaryOperator.identity() because the Java 8 developers failed to spot
     * that Function.identity() should return UnaryOperator.identity() result!  This is a nasty gotcha!
     *
     * @param f function reference to be checked
     *
     * @return true f reference equals Function.identity() or UnaryOperator.identity();
     */
    public static boolean isIdentityFunction(final Function<?, ?> f) {
        return f == Function.identity() || f == UnaryOperator.identity();
    }

    private boolean isNew() {
        return ++shareCount == 0;
    }

    /**
     * Assumes that caller already checked that mapper and collectorR are the same.
     *
     * @param andThenR  the unary operator processing R e.g. a sort function.
     * @param andThenRR the function to convert R to RR e.g. a percentile function.
     *
     * @return a stub collector only converting R to RR.
     */
    public Collector<T, ?, RR> share(final UnaryOperator<R> andThenR, // e.g. sort a list or Function.identity() if not needed.
                                     final Function<R, RR> andThenRR) {
        // See if 1st andThenR can be share for all columns e.g. only sort a list once.
        sameAndThenR &= andThenR == this.andThenR;
        // Create stub collector, where only the finisher does work.
        return Collector.of(
                () -> null,
                (a, t) -> {
                },
                (a1, a2) -> null,
                a -> sameAndThenR
                     ? andThenRR.apply(r)
                     : andThenRR.apply(andThenR.apply(r)));
    }

    //

    @Override
    public Supplier<A> supplier() {
        return supplier;
    }

    @Override
    public BiConsumer<A, T> accumulator() {
        return accumulator;
    }

    @Override
    public BinaryOperator<A> combiner() {
        return combiner;
    }

    @Override
    public Function<A, RR> finisher() {
        // Lazy create finisher, so can pre-chain protection when andThenR's differ in later columns.
        if (null == finisher)
            createFinisher();
        return finisher;
    }

    private void createFinisher() {
        Function<A, R> finisherA2R = (Function<A, R>) collectorR.finisher();
        if (!sameAndThenR)
            finisherA2R = finisherA2R.andThen(r -> {
                if (r instanceof List)
                    r = (R) Collections.unmodifiableList((List<?>) r);
                else if (r instanceof Map) // May not be useful
                    r = (R) Collections.unmodifiableMap((Map<?, ?>) r);
                else if (r instanceof Set) // May not be useful
                    r = (R) Collections.unmodifiableSet((Set<?>) r);
                this.r = r;
                return r;
            });
        if (!isIdentityFunction(finisherA2R))
            finisherA2R = finisherA2R.andThen(andThenR);
        if (sameAndThenR)
            finisherA2R = finisherA2R.andThen(r -> this.r = r); // andThenR The same in shared ones, so re-share.
        this.finisher = finisherA2R.andThen(andThenRR);
    }

    @Override
    public Set<Characteristics> characteristics() {
        return collectorR.characteristics();
    }

    /**
     * Holds a map of Main by Id.
     * <p>
     * Should only be used for one Stream or Seq. e.g. maybe a good idea to use a different instance for a Window.
     * <p>
     * Not Thread-safe, and doesn't need to be.
     */
    public static class IdMap {
        private final Map<String, SharableCollector<?, ?, ?, ?, ?>> map = new HashMap<>();

        public <T, R, RR> Collector<T, ?, RR> share(final String id,
                                                    final Collector<T, ?, R> collectorR,
                                                    final UnaryOperator<R> andThenR,
                                                    final Function<R, RR> andThenRR) {
            return share(id, Function.identity(), collectorR, andThenR, andThenRR);
        }

        /**
         * @param id         the id of shared collector.
         * @param mapper     Function.identity() or T to U mapper, to extract U value from T row.
         * @param collectorR the collector providing the R result from U values.
         * @param andThenR   the unary operator processing R e.g. a sort and/or protect function.
         * @param andThenRR  the function to convert R to RR e.g. a percentile function.
         * @param <T>        source value type.
         * @param <U>        accumulated value type.
         * @param <R>        collector result type.
         * @param <RR>       final result type.
         *
         * @return a SharableCollector instance or a stub collector only converting R to RR.
         */
        @SuppressWarnings("unchecked")
        public <T, U, R, RR> Collector<T, ?, RR> share(final String id,
                                                       final Function<T, U> mapper,
                                                       final Collector<U, ?, R> collectorR,
                                                       final UnaryOperator<R> andThenR,
                                                       final Function<R, RR> andThenRR) {
            // Only need to validate values here because everything it calls is private.
            if (Objects.requireNonNull(id, "id").length() == 0)
                throw new IllegalStateException("Blank id");
            final SharableCollector<T, U, ?, R, RR> sc = (SharableCollector<T, U, ?, R, RR>) map
                    .computeIfAbsent(id, k -> SharableCollector.of(mapper, collectorR, andThenR, andThenRR));
            if (sc.isNew())
                return sc;
            if (mapper != sc.mapper)
                throw new IllegalArgumentException(format("mapper %s not %s",
                                                          mapper, sc.mapper));
            if (collectorR.getClass() != sc.collectorR.getClass())
                throw new IllegalArgumentException(format("collector %s not a %s",
                                                          collectorR.getClass(), sc.collectorR.getClass()));
            return sc.share(andThenR, andThenRR);
        }
    }

    /**
     * @param mapper     Function.identity() or T to U mapper, to extract U value from T row.
     * @param collectorR the collector providing the R result, collects U.
     * @param andThenR   the unary operator processing R e.g. a list sort
     * @param andThenRR  the function to convert R to RR e.g. a percentile function.
     */
    public static <T, U, A, R, RR> SharableCollector<T, U, A, R, RR>
    of(final Function<T, U> mapper,
       final Collector<U, ?, R> collectorR,
       final UnaryOperator<R> andThenR,
       final Function<R, RR> andThenRR) {
        return new SharableCollector<>(mapper, collectorR, andThenR, andThenRR);
    }

    /**
     * @param collectorR the collector providing the R result, collects T.
     * @param andThenR   the unary operator processing R e.g. a list sort
     * @param andThenRR  the function to convert R to RR e.g. a percentile function.
     */
    public static <T, A, R, RR> SharableCollector<T, T, A, R, RR>
    of(final Collector<T, ?, R> collectorR,
       final UnaryOperator<R> andThenR,
       final Function<R, RR> andThenRR) {
        return new SharableCollector<>(Function.identity(), collectorR, andThenR, andThenRR);
    }
}
