use super::strategy::Tablet;
use super::tablet::State;
use crate::record::mow::mow;
use crate::record::sow::sow;
use crate::types::entry::Entry;
use crate::types::grain::Grain;
use crate::types::line::Line;
use async_stream::stream;
use futures_core::stream::Stream;
use futures_util::stream::StreamExt;
use regex::Regex;
use std::collections::HashMap;

fn make_state_initial(state: State, tablet: Tablet) -> State {
    // in a querying tablet, set initial entry to the base of the tablet
    // and preserve the received entry for sowing grains later
    // if tablet base is different from previous entry base
    // sow previous entry into the initial entry
    let is_same_base = tablet.querying && tablet.base == state.clone().query.unwrap().base;

    let do_discard = state.entry.is_none() || is_same_base;

    let entry_fallback = if do_discard {
        Entry {
            base: tablet.clone().base,
            base_value: None,
            leader_value: None,
            leaves: HashMap::new(),
        }
    } else {
        state.clone().entry.unwrap()
    };

    let do_sow = tablet.querying && !do_discard;

    let entry_initial = if do_sow {
        let foo = Entry {
            base: tablet.clone().base,
            base_value: None,
            leader_value: None,
            leaves: HashMap::new(),
        };

        let bar = Grain {
            base: tablet.clone().base,
            base_value: None,
            leaf: state.entry.clone().unwrap().base,
            leaf_value: state.entry.clone().unwrap().base_value,
        };

        sow(foo, bar, &tablet.base, &state.entry.clone().unwrap().base)
    } else {
        entry_fallback
    };

    let entry_base_changed =
        state.entry.is_none() || state.entry.clone().unwrap().base != entry_initial.base;

    // if entry base changed forget thingQuerying
    let thing_querying_initial = if entry_base_changed {
        None
    } else {
        state.clone().thing_querying
    };

    let is_value_tablet = !tablet.accumulating && !tablet.querying;

    let is_accumulating_by_trunk = tablet.accumulating && !tablet.thing_is_first;

    // in a value tablet use entry as a query
    let do_swap = is_value_tablet || is_accumulating_by_trunk;

    // TODO should this be Option instead and pass state.query without unwrapping?
    let query_initial = if do_swap {
        entry_initial.clone()
    } else {
        state.query.unwrap()
    };

    State {
        query: Some(query_initial),
        entry: Some(entry_initial),
        fst: None,
        is_match: false,
        has_match: false,
        match_map: state.match_map,
        thing_querying: thing_querying_initial,
    }
}

fn make_state_line(
    state_initial: State,
    state_old: State,
    tablet: Tablet,
    grains: Vec<Grain>,
    trait_: String,
    thing: String,
) -> State {
    let mut state = state_old.clone();

    let grain_new = Grain {
        base: tablet.clone().trait_,
        base_value: Some(trait_.clone()),
        leaf: tablet.clone().thing,
        leaf_value: Some(thing.clone()),
    };

    let grains_new: Vec<Grain> = grains
        .iter()
        .filter_map(|grain| {
            let is_match_grain = if tablet.trait_is_regex {
                let re_str = grain.clone().base_value.unwrap();

                let re = Regex::new(&re_str).unwrap();

                re.is_match(&trait_)
            } else {
                grain.clone().base_value.unwrap() == trait_
            };

            // when querying also match literal trait from the query
            // otherwise always true
            let do_diff = tablet.querying && state_initial.thing_querying.is_some();

            let is_match_querying = if do_diff {
                state_initial.clone().thing_querying.unwrap() == thing
            } else {
                true
            };

            let is_match = is_match_grain && is_match_querying;

            // accumulating tablets find all values
            // matched at least once across the dataset
            // check here if thing was matched before
            // this will always be true for non-accumulating maps
            // so will be ignored
            let match_is_new =
                state.match_map.is_none() || state.match_map.clone().unwrap().get(&thing).is_none();

            state.is_match = if state.is_match {
                state.is_match
            } else {
                is_match && match_is_new
            };

            if tablet.querying && state.is_match {
                state.thing_querying = Some(thing.clone())
            }

            if is_match && match_is_new && tablet.accumulating {
                state
                    .match_map
                    .as_mut()
                    .unwrap()
                    .insert(thing.clone(), true);
            }

            state.has_match = if state.has_match {
                state.has_match
            } else {
                state.is_match
            };

            if is_match && match_is_new {
                return Some(grain_new.clone());
            }

            None
        })
        .collect();

    state.entry = Some(
        grains_new
            .clone()
            .into_iter()
            .fold(state.entry.unwrap(), |with_grain, grain| {
                sow(with_grain, grain, &tablet.trait_, &tablet.thing)
            }),
    );

    if tablet.querying && thing == state_initial.thing_querying.unwrap() {
        // if previous querying tablet already matched thing
        // the trait in this record is likely to be the same
        // and might duplicate in the entry after sow
        return state;
    }

    state.query = Some(
        grains_new
            .iter()
            .fold(state.query.unwrap(), |with_grain, grain| {
                sow(with_grain, grain.clone(), &tablet.trait_, &tablet.thing)
            }),
    );

    state
}

pub fn select_line_stream<S: Stream<Item = Line>>(
    input: S,
    state: State,
    tablet: Tablet,
) -> impl Stream<Item = State> {
    let state_initial = make_state_initial(state.clone(), tablet.clone());

    let mut state_current = state_initial.clone();

    let grains = mow(
        state_current.query.clone().unwrap(),
        &tablet.trait_,
        &tablet.thing,
    );

    stream! {
        for await line in input {
            let fst_is_new = state_current.fst.is_some() && state_current.fst.unwrap() != line.key;

            state_current.fst = Some(line.clone().key);

            let is_complete = state_current.is_match;

            let is_end_of_group = tablet.eager && fst_is_new;

            let push_end_of_group = is_end_of_group && is_complete;

            if push_end_of_group {
                yield State {
                    query: state_current.clone().query,
                    entry: state_current.clone().entry,
                    fst: None,
                    is_match: false,
                    match_map: None,
                    has_match: false,
                    thing_querying: state_current.clone().thing_querying,
                };

                state_current.entry = state_initial.clone().entry;

                state_current.query = state_initial.clone().query;

                state_current.is_match = false;
            }

            let trait_ = if tablet.trait_is_first {line.clone().key} else {line.clone().value};

            let thing = if tablet.thing_is_first {line.clone().key} else {line.clone().value};

            let state_current = make_state_line(state_initial.clone(), state_current.clone(), tablet.clone(), grains.clone(), trait_, thing);
        }

        let is_complete = state_current.is_match;

        // we push at the end of non-eager tablet
        // because a non-eager tablet looks
        // for all possible matches until end of file
        // and doesn't push earlier than the end
        // push if tablet wasn't eager or if eager matched
        let push_end = !tablet.eager || is_complete;

        if is_complete {
            // don't push matchMap here
            // because accumulating is not yet finished
            yield State {
                query: state_current.clone().query,
                entry: state_current.clone().entry,
                thing_querying: state_current.clone().thing_querying,
                fst: None,
                is_match: false,
                has_match: false,
                match_map: None,
            }
        }

        let is_empty_passthrough = tablet.passthrough && !state.has_match;

        // after all records have been pushed for forwarding
        // push the matchMap so that other accumulating tablets
        // can search for new values
        if tablet.accumulating {
            // in accumulating by trunk this pushes entryInitial
            // to output and yields extra search result
            yield State {
                query: state_current.query,
                entry: state_initial.entry,
                match_map: state_current.match_map,
                fst: None,
                is_match: false,
                has_match: false,
                thing_querying: None
            };
        } else if is_empty_passthrough {
            yield State {
                query: state_current.query,
                entry: state_current.entry,
                match_map: None,
                fst: None,
                is_match: false,
                has_match: false,
                thing_querying: None
            };
        }
    }
}
