package rwperrott.lambda;

import org.jooq.lambda.tuple.Tuple2;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.function.UnaryOperator;

public final class Finishers {
    private Finishers() {
    }

    private static final class ListSortKey<T, U> extends Tuple2<Function<? super T, ? extends U>, Comparator<? super U>> {
        private ListSortKey(final Function<? super T, ? extends U> keyExtractor, final Comparator<? super U> keyComparator) {
            super(keyExtractor, keyComparator);
        }

        @SuppressWarnings({"unchecked", "rawtypes"})
        private static final ListSortKey NATURAL = new ListSortKey(Function.identity(), Comparator.naturalOrder());

        @SuppressWarnings("unchecked")
        private static <T, U> ListSortKey<T, U> of(final Function<? super T, ? extends U> keyExtractor, final Comparator<? super U> keyComparator) {
            return keyExtractor == Function.identity() && keyComparator == Comparator.naturalOrder()
                   ? (ListSortKey<T, U>) NATURAL
                   : new ListSortKey<>(keyExtractor, keyComparator);
        }
    }

    private static final UnaryOperator<? extends List<?>> NATURAL_LIST_SORTER = createSortList(Comparator.naturalOrder());
    private static final Map<ListSortKey<?, ?>, UnaryOperator<?>> LIST_SORTERS = new ConcurrentHashMap<>();

    public static <T, U> UnaryOperator<List<T>> sortList(final Function<? super T, ? extends U> keyExtractor,
                                                         final Comparator<? super U> keyComparator) {
        return sortList0(ListSortKey.of(keyExtractor, keyComparator));
    }

    /**
     * Caches keyExtractor and keyComparator combinations, so can detect same combinations for nextFinisherR2R, so can
     * avoid redundant re-sorting.
     * <p>
     * New ArrayList copy is only created if a sort was require and original list was unmodifiable, this allows
     * resorting of the same list, if you trust all the R2RR finishers.
     * <p>
     * Use as in R2RR function, if need a sorted list.
     *
     * @return the sorted original list or a sorted new ArrayList copy;
     */
    @SuppressWarnings("unchecked")
    private static <T, U> UnaryOperator<List<T>> sortList0(ListSortKey<T, U> key) {
        if (Objects.equals(ListSortKey.NATURAL, key))
            return (UnaryOperator<List<T>>) NATURAL_LIST_SORTER;
        return (UnaryOperator<List<T>>) LIST_SORTERS.computeIfAbsent(key, k -> {
            final Comparator<? super T> comp = key.v1 == Function.identity()
                                               ? (Comparator<? super T>) key.v2
                                               : Comparator.comparing(key.v1, key.v2);
            return createSortList(comp);
        });
    }

    private static <T> UnaryOperator<List<T>> createSortList(final Comparator<? super T> comp) {
        return l -> {
            final int size = l.size();
            if (size > 1) { // Only sort if 2 or more items.
                try { // Test if modifiable
                    l.set(0,l.get(0));
                } catch (Exception e) { // Unmodifiable, so copy.
                    l = new ArrayList<>(l);
                }
                l.sort(comp);
            }
            return l;
        };
    }

    @SuppressWarnings("unchecked")
    public static <T, U> UnaryOperator<List<T>> sortList(final Function<? super T, ? extends U> keyExtractor) {
        return sortList0(ListSortKey.of(keyExtractor, (Comparator<? super U>) Comparator.naturalOrder()));
    }

    public static <T> UnaryOperator<List<T>> sortList(final Comparator<? super T> keyComparator) {
        return sortList0(ListSortKey.of(Function.identity(), keyComparator));
    }

    @SuppressWarnings("unchecked")
    public static <T extends Comparable<? super T>> UnaryOperator<List<T>> sortList() {
        return (UnaryOperator<List<T>>) NATURAL_LIST_SORTER;
    }

    /**
     * Builds a function which accepts a sorted list and returns the percentile result.
     *
     * Usable for both simple and keyExtracted results.
     *
     * @param percentile 0 to 1.0
     * @param percentileFunction function to resolve indexFraction
     * @param <T> type
     * @return a function which accepts a sorted list and returns the percentile result.
     */
    public static <T> Function<List<T>, Optional<T>>
    percentile(double percentile, PercentileFunction<T> percentileFunction) {
        return l -> {
            final int size = l.size();

            if (size == 0)
                return Optional.empty();
            else if (size == 1)
                return Optional.of(l.get(0));

            if (percentile == 0d)
                return Optional.of(l.get(0));
            if (percentile == 1d)
                return Optional.of(l.get(size - 1));

            // Limit fraction size, to stop common errors for double percentile values e.g. 2E-16.
            // 0.5d is added because actual percentile value can be between values.
            final double dIndex = ((double) Math.round(size * percentile * 1.0E6d) * 1.0E-6d) - 0.5d;
            int index = (int) dIndex; // floor, for before or exact index
            if (index >= size)
                return Optional.of(l.get(size - 1));

            final T t0 = l.get(index); // 1st before or exact value
            final double indexFraction = dIndex - index;
            // If end or exact index, return t0 value.
            if (++index == size || indexFraction == 0d)
                return Optional.of(t0);

            final T t1 = l.get(index); // after value
            // Only call percentile function if t*.v1 values are different.
            return (t0.equals(t1))
                   ? Optional.of(t0)
                   : Optional.of(percentileFunction.apply(t0, t1, indexFraction));

        };
    }
}
