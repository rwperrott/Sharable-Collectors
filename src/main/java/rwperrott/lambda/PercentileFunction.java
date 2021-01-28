package rwperrott.lambda;


@FunctionalInterface
public interface PercentileFunction<T> {
    T apply(T t0,
            T t1,
            double indexFraction); // mapper proved to be redundant!

    static <T> PercentileFunction<T> floor() {
        return (t0, t1, f) -> t0;
    }

    static <T> PercentileFunction<T> ceil() {
        return (t0, t1, f) -> t1;
    }

    static <T> PercentileFunction<T> halfUp() {
        return (t0, t1, f) -> f < 0.5d ? t0 : t1;
    }

    static PercentileFunction<Double> interpolateDouble() {
        return (t0, t1, f) -> t0 - (t0 * f) + (t1 * f);
    }
}
