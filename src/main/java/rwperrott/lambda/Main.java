package rwperrott.lambda;

import org.jooq.lambda.Agg;
import org.jooq.lambda.Seq;
import org.jooq.lambda.tuple.*;

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;

import static org.jooq.lambda.tuple.Tuple.tuple;

public class Main {
    static final Double[] values = {0d, 10d, 20d, 30d, 40d, 50d, 60d, 70d, 80d, 90d};

    static final List<Tuple2<String, Double>> namedValues = Arrays.asList(
            tuple("Zero    ", 0d),
            tuple("Ten    ", 10d),
            tuple("Twenty ", 20d),
            tuple("Thirty ", 30d),
            tuple("Forty  ", 40d),
            tuple("Fifth  ", 50d),
            tuple("Sixty  ", 60d),
            tuple("Seventy", 70d),
            tuple("Eighty ", 80d),
            tuple("Ninety ", 90d));

    public static void main(String[] args) {
        collectForTest();
        collectForIdTest();
        collectByTest();
        collectByIdTest();
    }

    private static void collectForTest() {
        System.out.println();
        System.out.println("collectForTest");

        // Defined separately so that SharableCollector can eliminate redundant code for dependent columns.
        final UnaryOperator<List<Double>> andThenR = Finishers.sortList();

        final String header = "percentile -> |  Agg | floor | halfUp | interpolate | ceil";
        System.out.println(header);
        for (double p = 0d; p <= 1.00d; p += 0.05d) {
            final SharableCollector<Double, Double, ?, List<Double>, Optional<Double>> col =
                    SharableCollector.of(Collectors.toList(),
                                         andThenR,
                                         Finishers.percentile(p, PercentileFunction.floor()));
            final Tuple5<Optional<Double>, Optional<Double>, Optional<Double>, Optional<Double>, Optional<Double>> r =
                    Seq.of(values)
                       .collect(Tuple.collectors(
                               Agg.<Double>percentile(p, Comparator.naturalOrder()),
                               col,
                               col.share(andThenR, Finishers.percentile(p, PercentileFunction.halfUp())),
                               col.share(andThenR, Finishers.percentile(p, PercentileFunction.interpolateDouble())),
                               col.share(andThenR, Finishers.percentile(p, PercentileFunction.ceil()))
                                                ));
            System.out.printf("   %5.3f   -> | %4.1f |  %4.1f |   %4.1f |    %4.1f     | %4.1f%n",
                              p,
                              r.v1.orElse(0d),
                              r.v2.orElse(0d),
                              r.v3.orElse(0d),
                              r.v4.orElse(0d),
                              r.v5.orElse(0d));
        }
        System.out.println(header);
    }

    private static void collectForIdTest() {
        System.out.println();
        System.out.println("collectForIdTest");
        // Defined separately so that SharableCollector can eliminate redundant code for dependent columns.
        final UnaryOperator<List<Double>> andThenR = Finishers.sortList();

        final String header = "percentile -> |  Agg | floor | halfUp | interpolate | ceil";
        System.out.println(header);
        for (double p = 0d; p <= 1.00d; p += 0.05d) {
            final SharableCollector.IdMap idMap = new SharableCollector.IdMap();
            final Tuple5<Optional<Double>, Optional<Double>, Optional<Double>, Optional<Double>, Optional<Double>> r =
                    Seq.of(values)
                       .collect(Tuple.collectors(
                               Agg.<Double>percentile(p, Comparator.naturalOrder()),
                               idMap.share("A", Collectors.toList(), andThenR, Finishers.percentile(p, PercentileFunction.floor())),
                               idMap.share("A", Collectors.toList(), andThenR, Finishers.percentile(p, PercentileFunction.halfUp())),
                               idMap.share("A", Collectors.toList(), andThenR, Finishers.percentile(p, PercentileFunction.interpolateDouble())),
                               idMap.share("A", Collectors.toList(), andThenR, Finishers.percentile(p, PercentileFunction.ceil()))
                                                ));
            System.out.printf("   %5.3f   -> | %4.1f |  %4.1f |   %4.1f |    %4.1f     | %4.1f%n",
                              p,
                              r.v1.orElse(0d),
                              r.v2.orElse(0d),
                              r.v3.orElse(0d),
                              r.v4.orElse(0d),
                              r.v5.orElse(0d));
        }
        System.out.println(header);
    }

    private static void collectByTest() {
        System.out.println();
        System.out.println("collectByTest");

        // Defined separately so that SharableCollector can eliminate redundant code for dependent columns.
        final Function<Tuple2<String, Double>, Double> keyExtractor = t -> t.v2;
        // Defined separately so that SharableCollector can eliminate redundant code for dependent columns.
        final UnaryOperator<List<Tuple2<String, Double>>> andThenR = Finishers.sortList(keyExtractor);

        final String header = "percentile -> | Agg             | floor           | halfUp          | ceil";
        System.out.println(header);
        for (double p = 0d; p <= 1.00d; p += 0.05d) {
            final SharableCollector<Tuple2<String, Double>, Tuple2<String, Double>, Object, List<Tuple2<String, Double>>, Optional<Tuple2<String, Double>>> col =
                    SharableCollector.of(Collectors.toList(), andThenR, Finishers.percentile(p, PercentileFunction.floor()));
            final Tuple4<Optional<Tuple2<String, Double>>,
                    Optional<Tuple2<String, Double>>,
                    Optional<Tuple2<String, Double>>,
                    Optional<Tuple2<String, Double>>> r =
                    Seq.seq(namedValues)
                       .collect(Tuple.collectors(
                               Agg.percentileBy(p, keyExtractor),
                               col,
                               col.share(andThenR, Finishers.percentile(p, PercentileFunction.halfUp())),
                               col.share(andThenR, Finishers.percentile(p, PercentileFunction.ceil()))
                                                ));
            System.out.printf("   %5.3f   -> | %s | %s | %s | %s %n",
                              p,
                              r.v1.orElse(null),
                              r.v2.orElse(null),
                              r.v3.orElse(null),
                              r.v4.orElse(null));
        }
        System.out.println(header);
    }

    private static void collectByIdTest() {
        System.out.println();
        System.out.println("collectByIdTest");

        // Defined separately so that SharableCollector can eliminate redundant code for dependent columns.
        final Function<Tuple2<String, Double>, Double> keyExtractor = t -> t.v2;
        // Defined separately so that SharableCollector can eliminate redundant code for dependent columns.
        final UnaryOperator<List<Tuple2<String, Double>>> andThenR = Finishers.sortList(keyExtractor);

        final String header = "percentile -> | Agg             | floor           | halfUp          | ceil";
        System.out.println(header);
        for (double p = 0d; p <= 1.00d; p += 0.05d) {
            final SharableCollector.IdMap idMap = new SharableCollector.IdMap();
            final Tuple4<Optional<Tuple2<String, Double>>,
                    Optional<Tuple2<String, Double>>,
                    Optional<Tuple2<String, Double>>,
                    Optional<Tuple2<String, Double>>> r =
                    Seq.seq(namedValues)
                       .collect(Tuple.collectors(
                               Agg.percentileBy(p, keyExtractor),
                               idMap.share("A", Collectors.toList(), andThenR, Finishers.percentile(p, PercentileFunction.floor())),
                               idMap.share("A", Collectors.toList(), andThenR, Finishers.percentile(p, PercentileFunction.halfUp())),
                               idMap.share("A", Collectors.toList(), andThenR, Finishers.percentile(p, PercentileFunction.ceil()))
                                                ));
            System.out.printf("   %5.3f   -> | %s | %s | %s | %s %n",
                              p,
                              r.v1.orElse(null),
                              r.v2.orElse(null),
                              r.v3.orElse(null),
                              r.v4.orElse(null));
        }
        System.out.println(header);
    }

}
