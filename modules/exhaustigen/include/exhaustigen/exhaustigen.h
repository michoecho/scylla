// SPDX-License-Identifier: Apache-2.0
// Copyright Graydon Hoare <graydon@pobox.com>
//
// This file is a modified version of exhaustigen.hpp from
// https://github.com/graydon/exhaustigen. Full licence text in LICENSE beside
// this module.
//
// ---
//
// An exhaustive sequence generator: enumerate every combination of a set of
// small choices by writing the code that *makes* the choices, once.
//
// The generator is driven from a do-while loop, and each gen() call is a
// choice point:
//
//     Gen g;
//     do {
//         std::vector<size_t> v = g.gen_vec(3, 3);
//         check(v);
//     } while (!g.is_done());
//
// The first pass takes 0 at every choice point; each is_done() advances the
// rightmost choice that has room left, discards everything after it, and
// returns false. So the body runs once per point in the product of all the
// bounds, and the bounds may depend on earlier choices -- which is what makes
// this different from a nest of for loops.
//
// It must be a do-while: at the head of a while loop the generator has made no
// choices yet, is_done() sees an empty vector and reports done immediately.

#pragma once

#include <cstddef>
#include <limits>
#include <utility>
#include <vector>

namespace exhaustigen {

class Gen {
    // One entry per choice point reached this pass: the value taken, and the
    // inclusive upper bound it was taken against. The bound is recorded rather
    // than declared up front because it may differ between passes.
    std::vector<std::pair<size_t, size_t>> v;
    // How far into `v` this pass has got.
    size_t p{0};

public:
    // Advance to the next combination, or report that every one has been
    // visited. Call it as the condition of the driving do-while loop.
    bool is_done() {
        for (auto i = v.rbegin(); i != v.rend(); ++i) {
            if (i->first < i->second) {
                i->first += 1;
                // Everything after the advanced choice point is invalidated:
                // later bounds may depend on this choice, so those entries are
                // dropped and re-made on the next pass.
                v.erase(i.base(), v.end());
                p = 0;
                return false;
            }
        }
        return true;
    }

    // A value in [0, inclusive_upper_bound]. Every value in that range is
    // taken by some pass of the loop.
    size_t gen(size_t inclusive_upper_bound) {
        if (p == v.size()) {
            v.emplace_back(0, 0);
        }
        p += 1;
        auto& pair = v.at(p - 1);
        pair.second = inclusive_upper_bound;
        return pair.first;
    }

    /////////////////////////////////
    // Utility methods
    /////////////////////////////////

    bool flip() { return gen(1) == 1; }

    // A vector of length in [0, len_bound], with elements in [0, elt_bound].
    std::vector<size_t> gen_vec(size_t len_bound, size_t elt_bound) {
        std::vector<size_t> r(gen(len_bound), 0);
        for (auto& v : r) {
            v = gen(elt_bound);
        }
        return r;
    }

    // A combination: up to in.size() elements drawn from `in`, with repeats.
    template <typename T>
    std::vector<T> gen_comb(const std::vector<T>& in) {
        std::vector<T> r;
        const size_t sz = in.size();
        if (sz > 0) {
            const size_t n = gen(sz);
            for (size_t i = 0; i < n; ++i) {
                r.emplace_back(in.at(gen(sz - 1)));
            }
        }
        return r;
    }

    // A permutation of `in`: every ordering, each exactly once.
    template <typename T>
    std::vector<T> gen_perm(const std::vector<T>& in) {
        std::vector<T> r;
        const size_t sz = in.size();
        if (sz > 0) {
            std::vector<size_t> idxs;
            for (size_t i = 0; i < sz; ++i) {
                idxs.emplace_back(i);
            }
            while (!idxs.empty()) {
                auto ix = idxs.begin() + gen(idxs.size() - 1);
                r.emplace_back(in.at(*ix));
                idxs.erase(ix);
            }
        }
        return r;
    }

    // Every vector of exactly `k` elements, each in [min, max], summing to `n`.
    //
    // Order matters: 1+2 and 2+1 are separate passes. If nothing sums to `n`
    // under those bounds -- k*min > n, or k*max < n -- the result is empty on
    // every pass, and the loop runs once: there is no choice point to
    // enumerate. Note that this empty vector is not a split of `n` unless `n`
    // and `k` are both zero, so a caller that cares must check.
    //
    // Each element is a choice point, bounded so that every pass produces a
    // valid split rather than a candidate that has to be filtered: an element
    // can be no larger than what the slots after it are able to leave behind,
    // and no smaller than what they are able to absorb.
    //
    // With the default bounds the enumeration has C(n+k-1, k-1) passes -- the
    // stars-and-bars count of the ways to write `n` as `k` ordered addends.
    std::vector<size_t> gen_splits(
        size_t n, size_t k, size_t min = 0,
        size_t max = std::numeric_limits<size_t>::max()) {
        std::vector<size_t> r;
        // Feasible iff k*min <= n <= k*max, tested without forming either
        // product: both overflow for a large bound and a large `k`.
        const bool reaches_up =
            max == 0 ? n == 0 : k >= n / max + (n % max != 0);
        if (min > max || !reaches_up || (min != 0 && k > n / min)) {
            return r;
        }
        size_t left = n;
        for (size_t slots = k; slots > 0; --slots) {
            // What the remaining slots after this one can absorb. Neither
            // product below is formed unless it is already known to fit in
            // `left`, so neither can overflow.
            const size_t rest = slots - 1;
            const bool rest_covers_all =
                max == 0 ? left == 0 : rest >= left / max + (left % max != 0);
            const size_t floor_ = rest_covers_all ? 0 : left - rest * max;
            const size_t lo = floor_ > min ? floor_ : min;
            const size_t rest_min = min == 0 ? 0 : rest * min;
            const size_t hi = (left - rest_min < max) ? left - rest_min : max;
            const size_t e = lo + gen(hi - lo);
            r.emplace_back(e);
            left -= e;
        }
        return r;
    }

    // A subset of `in`, order preserved: one pass per point of the power set.
    template <typename T>
    std::vector<T> gen_subset(const std::vector<T>& in) {
        std::vector<T> r;
        for (const auto& i : in) {
            if (flip()) {
                r.emplace_back(i);
            }
        }
        return r;
    }
};

}  // namespace exhaustigen