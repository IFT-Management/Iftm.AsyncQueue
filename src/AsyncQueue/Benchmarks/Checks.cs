using System;

static class Checks {
    public static void Expect<T>(this T x, T y) where T : IEquatable<T> {
        if (!x.Equals(y)) throw new Exception();
    }
}
