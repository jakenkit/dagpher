package dagpher

import "sort"

func Map[F, T any](s []F, f func(F) T) []T {
	ret := make([]T, 0, len(s))
	for _, v := range s {
		ret = append(ret, f(v))
	}
	return ret
}

func SliceToMap[T any, K comparable, V any](s []T, transform func(T) (K, V)) map[K]V {
	result := make(map[K]V, len(s))

	for i := range s {
		k, v := transform(s[i])
		result[k] = v
	}

	return result
}

func Clone[T any](s []T) []T {
	if s == nil {
		return nil
	}
	out := make([]T, len(s))
	copy(out, s)
	return out
}

func SortBy[T any](s []T, less func(a, b T) bool) {
	sort.Slice(s, func(i, j int) bool {
		return less(s[i], s[j])
	})
}
