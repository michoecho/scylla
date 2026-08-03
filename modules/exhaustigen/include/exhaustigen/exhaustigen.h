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