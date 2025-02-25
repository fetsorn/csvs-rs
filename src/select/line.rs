use super::types::state::State;
use super::types::tablet::Tablet;
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

fn make_state_initial(state: &State, tablet: &Tablet) -> State {
    // in a querying tablet, set initial entry to the base of the tablet
    // and preserve the received entry for sowing grains later
    // if tablet base is different from previous entry base
    // sow previous entry into the initial entry
    let is_same_base = tablet.querying
        && (state.query.is_some() && tablet.base == state.query.as_ref().unwrap().base);

    let do_discard = state.entry.is_none() || is_same_base;

    let entry_fallback = if do_discard {
        Entry {
            base: tablet.base.to_owned(),
            base_value: None,
            leader_value: None,
            leaves: HashMap::new(),
        }
    } else {
        state.entry.as_ref().unwrap().clone()
    };

    let do_sow = tablet.querying && !do_discard;

    let entry_initial = if do_sow {
        let foo = Entry {
            base: tablet.base.to_owned(),
            base_value: None,
            leader_value: None,
            leaves: HashMap::new(),
        };

        let bar = Grain {
            base: tablet.base.to_owned(),
            base_value: None,
            leaf: state.entry.as_ref().unwrap().base.to_owned(),
            leaf_value: state.entry.as_ref().unwrap().base_value.clone(),
        };

        let baz = sow(
            &foo,
            &bar,
            &tablet.base,
            &state.entry.as_ref().unwrap().base,
        );

        Some(baz)
    } else {
        Some(entry_fallback)
    };

    let entry_base_changed = state.entry.is_none()
        || state.entry.as_ref().unwrap().base != entry_initial.as_ref().unwrap().base;

    // if entry base changed forget thingQuerying
    let thing_querying_initial = if entry_base_changed {
        None
    } else {
        state.thing_querying.clone()
    };

    let is_value_tablet = !tablet.accumulating && !tablet.querying;

    let is_accumulating_by_trunk = tablet.accumulating && !tablet.thing_is_first;

    // in a value tablet use entry as a query
    let do_swap = is_value_tablet || is_accumulating_by_trunk;

    // TODO should this be Option instead and pass state.query without unwrapping?
    let query_initial = if do_swap {
        &entry_initial
    } else {
        &state.query
    };

    State {
        query: query_initial.clone(),
        entry: entry_initial.clone(),
        fst: None,
        is_match: false,
        has_match: false,
        match_map: state.match_map.clone(),
        thing_querying: thing_querying_initial,
    }
}

fn make_state_line(
    state_initial: &State,
    state: &mut State,
    tablet: &Tablet,
    grains: &Vec<Grain>,
    trait_: String,
    thing: String,
) {
    // if tablet.filename == "datum-filepath.csv" {
    // println!("{} {}", tablet.filename, serde_json::to_string_pretty(&grains).unwrap());
    // };

    let grain_new = Grain {
        base: tablet.trait_.to_owned(),
        base_value: Some(trait_.to_owned()),
        leaf: tablet.thing.to_owned(),
        leaf_value: Some(thing.to_owned()),
    };

    // if tablet.filename == "datum-filepath.csv" {println!("{} {}", tablet.filename, serde_json::to_string_pretty(&grain_new).unwrap())};
    for grain in grains {
        // println!("{} {}", tablet.filename, serde_json::to_string_pretty(&grain).unwrap());

        // println!("{} {} {}", tablet.filename, tablet.trait_, trait_);
        let foo = if tablet.trait_is_first {
            grain.base_value.as_ref()
        } else {
            grain.leaf_value.as_ref()
        };

        // if tablet.filename == "datum-filepath.csv" {println!("{} {} {:?}", tablet.filename, tablet.trait_is_first, foo.clone())};

        let is_match_grain = if tablet.trait_is_regex {
            let re_str = match foo {
                None => String::from(""),
                Some(s) => s.to_owned(),
            };

            let re = Regex::new(&re_str).unwrap();

            // if tablet.filename == "datum-filepath.csv" {println!("{} {} {} {}", tablet.filename, foo.unwrap_or("".to_owned()), trait_, re.is_match(&trait_))};

            re.is_match(&trait_)
        } else {
            // if tablet.filename == "datum-filepath.csv" {println!("{}, {}, {}, {}", tablet.filename, foo.clone().unwrap_or("".to_owned()), trait_, foo.clone().is_some() && foo.clone().unwrap() == trait_)};
            foo.is_some() && foo.unwrap() == &trait_
        };

        // println!("{} {}", tablet.filename, is_match_grain);

        // when querying also match literal trait from the query
        // otherwise always true
        let do_diff = tablet.querying && state_initial.thing_querying.is_some();

        let is_match_querying = if do_diff {
            state_initial.thing_querying.as_ref().unwrap() == &thing
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
            state.match_map.is_none() || state.match_map.as_ref().unwrap().get(&thing).is_none();

        state.is_match = if state.is_match {
            state.is_match
        } else {
            is_match && match_is_new
        };

        if tablet.querying && state.is_match {
            state.thing_querying = Some(thing.to_owned())
        }

        if is_match && match_is_new && tablet.accumulating {
            state
                .match_map
                .as_mut()
                .unwrap()
                .insert(thing.to_owned(), true);
        }

        state.has_match = if state.has_match {
            state.has_match
        } else {
            state.is_match
        };

        if is_match && match_is_new {
            state.entry = Some(sow(
                state.entry.as_ref().unwrap(),
                &grain_new,
                &tablet.trait_,
                &tablet.thing,
            ));

            if tablet.querying {
                // if previous querying tablet already matched thing
                // the trait in this record is likely to be the same
                // and might duplicate in the entry after sow
                if !(state_initial.thing_querying.is_some()
                    && thing == *state_initial.thing_querying.as_ref().unwrap())
                {
                    state.query = Some(sow(
                        state.query.as_ref().unwrap(),
                        &grain_new,
                        &tablet.trait_,
                        &tablet.thing,
                    ));
                }
            }
        }
    }

    // if tablet.filename == "datum-filepath.csv" {
    // println!("{} {}", tablet.filename, serde_json::to_string_pretty(&state.entry).unwrap());
    // };
}

pub fn select_line_stream<S: Stream<Item = Line>>(
    input: S,
    state: State,
    tablet: Tablet,
) -> impl Stream<Item = State> {
    //if tablet.filename == "datum-filepath.csv" {
    // println!("{} {}", tablet.filename, serde_json::to_string_pretty(&state).unwrap());
    //};

    let state_initial = make_state_initial(&state, &tablet);

    // if tablet.filename == "datum-filepath.csv" {println!("{} {}", tablet.filename, serde_json::to_string_pretty(&state_initial).unwrap())};

    let mut state_current = state_initial.clone();

    let grains = mow(
        state_current.query.as_ref().unwrap(),
        &tablet.trait_,
        &tablet.thing,
    );

    // if tablet.filename == "datum-filepath.csv" {println!("{} {}", tablet.filename, serde_json::to_string_pretty(&grains).unwrap())};

    stream! {
        for await line in input {

            // println!("{} {},{}", tablet.filename, line.key, line.value);
            // println!("{} {} {} {}", tablet.filename, tablet.passthrough, line.clone().key, line.clone().value);

            let fst_is_new = state_current.fst.is_some() && state_current.fst.unwrap() != line.key;

            // if tablet.filename == "datum-filepath.csv" {println!("{} {} {} {}", tablet.filename, line.key, line.value, fst_is_new)};

            state_current.fst = Some(line.key.to_owned());

            let is_complete = state_current.is_match;

            let is_end_of_group = tablet.eager && fst_is_new;

            let push_end_of_group = is_end_of_group && is_complete;

            // if tablet.filename == "datum-filepath.csv" {println!("{} {}", tablet.filename, push_end_of_group)};

            if push_end_of_group {
                 // if tablet.filename == "datum-filepath.csv" {
                     // println!("E {} {}", tablet.filename, serde_json::to_string_pretty(&state_current).unwrap());
                 // };

                // println!("{} {} 1", tablet.filename, tablet.passthrough);
                let state_to_push = State {
                    query: state_current.query,
                    entry: state_current.entry,
                    fst: None,
                    is_match: false,
                    match_map: None,
                    has_match: false,
                    thing_querying: state_current.thing_querying.clone(),
                };

                // println!("push end of group {} {},{} {}", tablet.filename, line.key, line.value, state_to_push);
                // if tablet.accumulating {println!("push end of group {} {},{} {}", tablet.filename, line.key, line.value, state_to_push)};

                yield state_to_push;
                // println!("{} {} 2", tablet.filename, tablet.passthrough);
                // if tablet.accumulating {println!("?" )};

                state_current.entry = state_initial.entry.clone();

                state_current.query = state_initial.query.clone();

                state_current.is_match = false;
            }

            let trait_ = if tablet.trait_is_first {line.key.to_owned()} else {line.value.to_owned()};

            let thing = if tablet.thing_is_first {line.key.to_owned()} else {line.value.to_owned()};

            // println!("{} {} {} {}", tablet.filename, tablet.passthrough, trait_, thing);

            // if tablet.accumulating {println!("{:?} \n {:#?}", tablet, line)};
            make_state_line(&state_initial, &mut state_current, &tablet, &grains, trait_, thing);

            // if tablet.filename == "datum-filepath.csv" {
                // println!("{} {}", tablet.filename, serde_json::to_string_pretty(&state_current).unwrap())
            // };
        }

        // println!("{} {} 3", tablet.filename, tablet.passthrough);

        let is_complete = state_current.is_match;

        // we push at the end of non-eager tablet
        // because a non-eager tablet looks
        // for all possible matches until end of file
        // and doesn't push earlier than the end
        // push if tablet wasn't eager or if eager matched
        let push_end = !tablet.eager || is_complete;

        if is_complete {
            // if tablet.filename == "datum-filepath.csv" {println!("C {} {}", tablet.filename, serde_json::to_string_pretty(&state_current).unwrap())};
            // don't push matchMap here
            // because accumulating is not yet finished
            let state_to_push = State {
                query: state_current.query.clone(),
                entry: state_current.entry.clone(),
                thing_querying: state_current.thing_querying,
                fst: None,
                is_match: false,
                has_match: false,
                match_map: None,
            };

            // println!("push end of file {} {}", tablet.filename, state_to_push);
            // if tablet.accumulating {println!("push end of file {} {}", tablet.filename, state_to_push)};

            yield state_to_push;
        }

        let is_empty_passthrough = tablet.passthrough && state_current.has_match == false;

        // after all records have been pushed for forwarding
        // push the matchMap so that other accumulating tablets
        // can search for new values
        if tablet.accumulating {
            // if tablet.filename == "datum-filepath.csv" {println!("A {} {}", tablet.filename, serde_json::to_string_pretty(&state_current).unwrap())};
            // in accumulating by trunk this pushes entryInitial
            // to output and yields extra search result
            let state_to_push = State {
                query: state_current.query,
                entry: state_initial.entry,
                match_map: state_current.match_map,
                fst: None,
                is_match: false,
                has_match: false,
                thing_querying: None
            };

            // println!("push matchMap {} {}", tablet.filename, state_to_push);

            yield state_to_push;
        } else if is_empty_passthrough {
            // if tablet.filename == "datum-filepath.csv" {println!("P {} {}", tablet.filename, serde_json::to_string_pretty(&state_current).unwrap())};
            let state_to_push = State {
                query: state_current.query,
                entry: state_current.entry,
                match_map: None,
                fst: None,
                is_match: false,
                has_match: false,
                thing_querying: None
            };

            // println!("forward empty {} {}", tablet.filename, state_to_push);
            // if tablet.accumulating {println!("forward empty {} {}", tablet.filename, state_to_push)};

            yield state_to_push;
        }
    }
}
