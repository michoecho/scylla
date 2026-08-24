//! LibAFL's in-process fuzzer, behind the C ABI declared in include/libafl_c.h.
//!
//! Read that header first: it explains why the boundary is shaped the way it is
//! (Rust owns the loop, the harness receives raw bytes, exceptions never cross).
//! This file is about how the pieces are assembled on the Rust side.
//!
//! The assembly is deliberately a small LibAFL fuzzer -- one observer, one
//! feedback, one scheduler, a corpus-trimming stage, and ordinary mutation.
//! LibAFL's whole selling point is that these are swappable.

use std::cell::{Cell, RefCell};
use std::ffi::{c_char, c_int, c_void, CString};
use std::os::raw::c_uchar;
use std::ptr;

use libafl::{
    corpus::InMemoryCorpus,
    events::SimpleEventManager,
    executors::{inprocess::InProcessExecutor, ExitKind},
    feedback_or_fast,
    feedbacks::{CrashFeedback, MaxMapFeedback},
    fuzzer::{Fuzzer, StdFuzzer},
    inputs::{BytesInput, HasTargetBytes},
    monitors::SimpleMonitor,
    mutators::{
        havoc_mutations::havoc_mutations,
        mutations::BytesDeleteMutator,
        MutationResult, Mutator,
        scheduled::HavocScheduledMutator,
    },
    schedulers::QueueScheduler,
    stages::{
        ObserverEqualityFactory, StdTMinMutationalStage,
        mutational::StdMutationalStage,
    },
    state::{HasRand, StdState},
    Error, Evaluator,
};
use libafl_bolts::{Named, rands::StdRand, tuples::tuple_list, AsSlice};
use libafl_targets::{edges_max_num, std_edges_map_observer};

/// `StdTMinMutationalStage` retries a skipped mutation without advancing its
/// run counter. `BytesDeleteMutator` skips inputs of length two or less, which
/// would make a trim stage spin forever once it had reduced the seed to that
/// size. Report a no-op as `Mutated` in that case; TMin will see that the input
/// did not get shorter and count the attempt without executing the harness.
#[derive(Debug)]
struct TrimBytesDeleteMutator;

impl Named for TrimBytesDeleteMutator {
    fn name(&self) -> &std::borrow::Cow<'static, str> {
        static NAME: std::borrow::Cow<'static, str> =
            std::borrow::Cow::Borrowed("TrimBytesDeleteMutator");
        &NAME
    }
}

impl<S> Mutator<BytesInput, S> for TrimBytesDeleteMutator
where
    S: HasRand,
{
    fn mutate(
        &mut self,
        state: &mut S,
        input: &mut BytesInput,
    ) -> Result<MutationResult, Error> {
        if input.target_bytes().as_slice().len() <= 2 {
            return Ok(MutationResult::Mutated);
        }

        BytesDeleteMutator::new().mutate(state, input)
    }

    fn post_exec(
        &mut self,
        _state: &mut S,
        _new_corpus_id: Option<libafl::corpus::CorpusId>,
    ) -> Result<(), Error> {
        Ok(())
    }
}

// --- the C ABI surface ------------------------------------------------------

/// Mirrors `LibAflHarnessResult`.
const HARNESS_OK: c_uchar = 0;
const HARNESS_FAILED: c_uchar = 1;

type HarnessFn = extern "C" fn(*const u8, usize, *mut c_void) -> c_uchar;

/// Mirrors `LibAflRunReport`.
#[repr(C)]
pub struct LibAflRunReport {
    invocations: u64,
    found_failure: u8,
}

thread_local! {
    /// The message from the last failed `libafl_c_run`, kept alive for
    /// `libafl_c_last_error` to hand back. Thread-local because the error
    /// belongs to the call that produced it, as the header promises.
    static LAST_ERROR: RefCell<Option<CString>> = const { RefCell::new(None) };
}

fn set_last_error(message: &str) {
    // A NUL inside the message would truncate it; replacing rather than failing
    // keeps error reporting itself from becoming an error path.
    let cleaned = message.replace('\0', "?");
    LAST_ERROR.with(|slot| {
        *slot.borrow_mut() = CString::new(cleaned).ok();
    });
}

/// See `libafl_c_last_error`.
#[unsafe(no_mangle)]
pub extern "C" fn libafl_c_last_error() -> *const c_char {
    LAST_ERROR.with(|slot| match slot.borrow().as_ref() {
        Some(message) => message.as_ptr(),
        None => ptr::null(),
    })
}

/// See `libafl_c_edge_count`.
///
/// This is how the C++ side tells an instrumented build from a plain one. It
/// reads the count SanCov's `_init` accumulated, which is zero when the binary
/// carries no `-fsanitize-coverage=trace-pc-guard` instrumentation at all.
#[unsafe(no_mangle)]
pub extern "C" fn libafl_c_edge_count() -> usize {
    edges_max_num()
}

/// The coverage map itself, for callers that want to inspect what an execution
/// touched rather than just how big the map is.
///
/// This exists for the gradient check described in modules/test_rng: whether a
/// longer correct prefix lights an edge no shorter one did is the property the
/// whole coverage-guided backend rests on, and the honest way to answer it is
/// to read the map around a call rather than infer it from whether a search
/// happened to succeed.
///
/// # Safety
///
/// The pointer is to a static map owned by libafl_targets. Writing through it
/// (to clear it between measurements) is sound only while no fuzzer is running.
#[unsafe(no_mangle)]
pub extern "C" fn libafl_c_edges_ptr() -> *mut u8 {
    unsafe { libafl_targets::edges_map_mut_ptr() }
}

/// The length of the map returned by [`libafl_c_edges_ptr`].
#[unsafe(no_mangle)]
pub extern "C" fn libafl_c_edges_len() -> usize {
    edges_max_num()
}

// --- the fuzzer -------------------------------------------------------------

/// See `libafl_c_run`.
///
/// # Safety
///
/// `harness` must be a valid function pointer, `ctx` is passed through to it
/// untouched, and `out_report` must point to a writable `LibAflRunReport`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn libafl_c_run(
    harness: HarnessFn,
    ctx: *mut c_void,
    max_invocations: u64,
    seed: u64,
    out_report: *mut LibAflRunReport,
) -> c_int {
    // The C++ side is entitled to a status code for every failure mode,
    // including a panic inside LibAFL. Panicking across the FFI boundary is
    // undefined behaviour in the same way an exception unwinding into Rust is,
    // so the whole run is wrapped.
    //
    // AssertUnwindSafe is justified because nothing observable outlives a
    // failed run: on the error path the report is not written and the fuzzer
    // state is dropped.
    let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        run_inner(harness, ctx, max_invocations, seed)
    }));

    match outcome {
        Ok(Ok(report)) => {
            if !out_report.is_null() {
                unsafe { ptr::write(out_report, report) };
            }
            0
        }
        Ok(Err(error)) => {
            set_last_error(&error.to_string());
            -1
        }
        Err(_) => {
            set_last_error("libafl: the fuzzer panicked");
            -2
        }
    }
}

fn run_inner(
    harness: HarnessFn,
    ctx: *mut c_void,
    max_invocations: u64,
    seed: u64,
) -> Result<LibAflRunReport, Error> {
    // Counted here rather than taken from LibAFL's own statistics, for the same
    // reason the C++ provider counts Hegel's invocations itself: this is the
    // number of times the *body* ran, which is what a RunReport promises, and
    // it is the only number both sides can agree on.
    //
    // Cell rather than plain locals because the harness closure below is held
    // mutably borrowed by the executor for as long as the fuzzer exists, while
    // the driving loop needs to read both values after every iteration. Shared
    // interior mutability is the ordinary answer; there is no thread here, so
    // Cell suffices and no synchronization is paid for.
    let invocations = Cell::new(0u64);
    let found_failure = Cell::new(false);

    // The bridge. Everything interesting about this closure is that it does not
    // interpret the bytes at all -- it hands them straight to C++, which carves
    // parameters out of them with the same logic the afl backend uses.
    let mut bridge = |input: &BytesInput| {
        let bytes = input.target_bytes();
        let slice = bytes.as_slice();
        invocations.set(invocations.get() + 1);

        // An empty testcase still has to reach the body: the C++ RNG treats a
        // short input as "draw zeroes past the end" (see AflRng), and refusing
        // to run one would hide the coverage that motivates growing it.
        let result = harness(slice.as_ptr(), slice.len(), ctx);

        if result == HARNESS_FAILED {
            found_failure.set(true);
            // Crash rather than Ok is what makes CrashFeedback fire, which is
            // what puts this input in the solutions corpus and ends the run.
            ExitKind::Crash
        } else {
            debug_assert_eq!(result, HARNESS_OK);
            ExitKind::Ok
        }
    };

    // The coverage map SanCov's runtime writes into. This observer is the
    // entire reason this backend beats a random search: without it MaxMapFeedback
    // has nothing to rate inputs by, and the fuzzer cannot tell a testcase that
    // reached new code from one that did not.
    //
    // SAFETY: the map is a static in libafl_targets, and only one fuzzer runs at
    // a time in this process (the C++ provider is not re-entrant).
    let edges_observer = unsafe { std_edges_map_observer("edges") };

    // Novelty search over that map: an input is "interesting", and therefore
    // kept in the corpus to mutate from, exactly when it lit a map entry no
    // earlier input did. This is the gradient the magic-bytes demo climbs.
    let mut feedback = MaxMapFeedback::new(&edges_observer);
    let trim_factory = ObserverEqualityFactory::new(&edges_observer);

    // What counts as a find. `feedback_or_fast` short-circuits, so the map is
    // not consulted once a crash is known.
    let mut objective = feedback_or_fast!(CrashFeedback::new());

    // Fixed seed, so a failing property fails on every run rather than one run
    // in fifty -- the same derandomization rationale as the Hegel backend's
    // settings.derandomize.
    let mut state = StdState::new(
        StdRand::with_seed(seed),
        // In-memory on both counts: this backend is a test that runs in a build
        // tree, and writing a corpus or a crash directory into it would leave
        // artefacts an ordinary `buck2 test` run has no way to clean up. The C++
        // side learns about a failure through the report, not through a file.
        InMemoryCorpus::new(),
        InMemoryCorpus::new(),
        &mut feedback,
        &mut objective,
    )?;

    let monitor = SimpleMonitor::new(|_| {
        // Silent. A passing search that printed statistics would be noise in a
        // test suite, which is the same call the Hegel backend makes.
    });
    let mut manager = SimpleEventManager::new(monitor);

    let scheduler = QueueScheduler::new();
    let mut fuzzer = StdFuzzer::new(scheduler, feedback, objective);

    let mut executor = InProcessExecutor::new(
        &mut bridge,
        tuple_list!(edges_observer),
        &mut fuzzer,
        &mut state,
        &mut manager,
    )?;

    // A seed input, rather than a generated corpus. One all-zero testcase is
    // enough: the body draws zeroes past the end of a short input anyway, so
    // this is the same starting point the smoke backend uses, and it leaves the
    // search with nothing but coverage feedback to climb -- which is precisely
    // what the backend is being asked to demonstrate.
    //
    // add_input rather than evaluate_input, and the difference is not a detail.
    // evaluate_input only files an input away if a feedback rated it
    // interesting, and the very first testcase is rated against an all-zero
    // coverage map, so a body whose first run happens to look unremarkable
    // leaves the corpus empty -- and a fuzzer with an empty corpus has nothing
    // to mutate and stops with "No entries in corpus. This often implies the
    // target is not properly instrumented", which is a misleading thing to be
    // told when the target is instrumented perfectly well. add_input is the
    // unconditional form and is what a seed wants.
    fuzzer.add_input(
        &mut state,
        &mut executor,
        &mut manager,
        BytesInput::new(vec![0u8; 1]),
    )?;

    // First try deleting bytes while preserving the exact edge map. The trim
    // stage replaces the corpus entry only when the deletion is coverage
    // equivalent, so minimization does not discard the path that made the
    // testcase interesting.
    let trim_stage = StdTMinMutationalStage::new(
        TrimBytesDeleteMutator,
        trim_factory,
        1,
    );

    let mutator = HavocScheduledMutator::new(havoc_mutations());
    let mut stages = tuple_list!(trim_stage, StdMutationalStage::new(mutator));

    // The loop. `fuzz_loop` would run forever, so the budget is enforced by
    // driving one iteration at a time and checking after each -- which also
    // gives the "stop at the first failure" behaviour the C++ RunReport
    // promises, without waiting for the fuzzer to notice on its own.
    //
    // The counts are approximate at the edges: one fuzz_one is a whole
    // mutational stage, so a run can overshoot max_invocations by less than the
    // stage's width. That is reported honestly rather than clamped, since the
    // body really did run that many times.
    loop {
        if found_failure.get() {
            break;
        }
        if max_invocations != 0 && invocations.get() >= max_invocations {
            break;
        }
        fuzzer.fuzz_one(&mut stages, &mut executor, &mut state, &mut manager)?;
    }

    Ok(LibAflRunReport {
        invocations: invocations.get(),
        found_failure: u8::from(found_failure.get()),
    })
}
