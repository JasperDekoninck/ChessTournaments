# Tournament pairing and round-scheduling audit — 27 September 2026

> Historical audit, recorded before the fixes in this change. The application now uses separate Dutch and team engines, validates results and round progression, and locks finished results. Current regression coverage is in `tests/test_pairing_rules.py`; the initial-colour and seeding conventions below remain unchanged. The archived reproduction scripts target the pre-fix implementation at base commit `79d35cf` and are not tests of the updated application.

**Conclusion: the implementation follows several basic Swiss principles, but does not implement the complete official FIDE Dutch or Swiss Team pairing systems correctly. There are also reproducible round-management defects.**

Reviewed the current workspace (base commit `79d35cf`, including the uncommitted registration-time and round-count changes). Compared with the FIDE rules effective 1 February 2026. The scope is opponent selection, colours, byes, result handling and round progression; this app does not schedule individual games by clock time.

All 87 automated tests passed in the reviewed workspace. Independent reproductions below demonstrate gaps in their coverage. Tests used temporary databases; the running local tournament was not modified. No application code was changed during this audit.

## Confirmed findings

### 1. Pairing selection stops before satisfying Dutch quality criteria

Location: `src/flaskr/core.py:1046`, `:1058`, `:1110`, `:1200`.

`_match_halves` chooses locally preferred opponents and returns its first complete legal matching. `_pair_group_matching` only tries exchanges outside the initial halves when that matching is impossible. It does not compare full candidates by the required ordered criteria. Previous upfloat/downfloat history is also absent from the pairing state.

Concrete reproduction: 12 players, pairing round two after these round-one results (White first):

| White | Black | Result |
| --- | --- | --- |
| 1 | 7 | draw |
| 8 | 2 | draw |
| 3 | 9 | 0–1 |
| 10 | 4 | draw |
| 5 | 11 | 1–0 |
| 12 | 6 | draw |

For the eight players on 0.5, the app produces `8–1, 2–10, 4–12, 6–7`. Players 8 and 7 receive their previous colour again. The legal alternative `2–1, 4–8, 6–10, 7–12` satisfies every colour preference, with identical scores, no rematches and no floaters. This demonstrates a violation of criterion C12 independently of deciding which equally optimal candidate wins the final tie-break.

Required change: evaluate complete brackets by all ordered Dutch criteria and implement the prescribed candidate ordering; include float history. Reference: [FIDE Dutch, articles 2.4, 3.4–3.8 and 4](https://handbook.fide.com/chapter/C0403202602).

### 2. Team tournaments use individual colour prohibitions

Location: `src/flaskr/core.py:902`, `:1305`; `src/flaskr/web.py:1586`.

The generator does not dispatch on `is_team`; team entries receive the same absolute colour prohibitions as individuals. In a six-team test, two rounds leave teams A and C with `WW` histories. All other teams then withdraw. A and C have not met, but round three returns no pairings because both prefer Black absolutely under the individual rules.

That is a legal team match: the team rules explicitly remove absolute colour prohibitions. Their bracket construction and colour priorities also differ from individual Dutch. Teams are currently treated as single players with one score, rather than implementing the full team system.

Required change: a separate team pairing policy/engine. Reference: [FIDE Swiss Team, preface and articles 1.7, 2–4](https://handbook.fide.com/chapter/SwissTeamPairingSystem202602).

### 3. Forfeits are counted as played games and do not disqualify a player from a bye

Location: `src/flaskr/core.py:497`, `:700`, `:1335`.

The result parser understands `1F-0F`, but standings add a colour and opponent encounter just as for a played game. `had_bye_before` only checks for a missing opponent.

Reproduction: A wins by forfeit, C wins a played game, E receives a bye; the two losing players withdraw. A, C and E each have one point. With A seeded below C, the generator gives A the next bye, even though C is the only eligible recipient. It also records A's forfeit as a White game and an opponent encounter.

Required change: represent played/unplayed results explicitly; exclude unplayed games from colour/opponent history and reject prior full-point unplayed winners when allocating byes. References: [Basic Swiss article 4](https://handbook.fide.com/chapter/C0401202507), [General Handling articles 3.4–3.5](https://handbook.fide.com/chapter/GeneralHandlingRulesForSwissTournaments202602).

### 4. Colour tie-breaking ignores the higher score

Location: `src/flaskr/core.py:953`, `:987`.

`_higher_ranked` compares only pairing numbers. Example: player #2 has 3 points and player #1 has 2 points; both have the same `WBWB` history and prefer White. The app gives White to #1. With the earlier colour rules tied, #2 should receive the preference because the score takes precedence over the pairing number.

Required change: use score, then pairing number, for this ranking. Reference: [FIDE Dutch articles 1.2 and 5.2.4](https://handbook.fide.com/chapter/C0403202602).

### 5. Manual saves bypass round sequencing and the planned-round limit

Location: `src/flaskr/web.py:1604`.

Automatic generation checks the next permitted round. Manual saves do not. Authenticated POSTs successfully saved round two before round one existed, and round 99 in a tournament configured for three rounds. Round 99 is persisted even though the admin only renders the planned rounds. Future-round panels also expose editable pairing controls.

Required change: validate round bounds and permitted progression on the server, with a separate explicit correction path for previously played rounds. Retain deliberate manual pairing overrides without allowing them to create invalid tournament states.

### 6. Invalid results are accepted as completed games

Location: `src/flaskr/core.py:1396`, `:1555`.

An authenticated round save accepted `banana`, stored `BANANA`, and reported the round complete. Automatic generation then created the next round. The score parser could not interpret that result, so the next pairing used incomplete scores.

Required change: validate supported result codes before saving; completion must require a valid result, not merely a nonempty string. Include explicit supported forfeit outcomes when fixing finding 3.

### 7. Editing finished results leaves final standings stale

Location: `src/flaskr/web.py:1604`, `:1633`; `src/flaskr/core.py:613`.

Reproduction through the routes: save a one-round tournament as `1-0`, finish it, then save the game as `0-1`. The game changes, but the tournament stays completed and stored final scores remain `1,0`. Recalculation gives `0,1`. Public final standings read the stored values. The round-save route also does not rebuild ratings for a rated event.

Required change: either reject changes after completion or implement a correction/reopening flow that refreshes final standings, ratings and derived exports consistently.

## Additional compliance considerations

- The initial colour is hard-coded to White (`core.py:20`); there is no setting to record the pre-event colour draw required by [Dutch article 5.1](https://handbook.fide.com/chapter/C0403202602).
- Initial order uses rating, name and entry ID, without the FIDE-title criterion. Pairing numbers are recomputed on every call without a participant-list freeze. Review this against [General Handling articles 2.2–2.5](https://handbook.fide.com/chapter/GeneralHandlingRulesForSwissTournaments202602).
- The requested round-count editing is useful for club events. For an official Swiss event, declare the number of rounds beforehand; changing it after play starts also changes which round permits the topscorer colour exceptions. See [Basic Swiss article 1](https://handbook.fide.com/chapter/C0401202507).
- Manual repeat-opponent overrides are intentional in the existing tests and produce a warning. An organizer can therefore deliberately override the normal no-rematch restriction; this is not automatic rules enforcement.

## What is covered and working

The current tests and source checks cover basic first-round splitting, automatic avoidance of repeat opponents, non-repetition of pairing-allocated byes, several individual colour-preference cases, absences/withdrawals, and automatic next-round gating for normal results. The random-tournament test exercises 20 tournaments of 48 players over seven rounds. These invariants do not verify exact Dutch candidate quality or team-specific rules.

## Reproduction and next steps

For the historical implementation, run from the repository root (the defect reproductions intentionally do not pass against the fixed implementation):

```bash
.venv/bin/python -m unittest discover -s tests
.venv/bin/python reports/pairing-audit-2026-09-27/find_pairing_counterexample.py
.venv/bin/python reports/pairing-audit-2026-09-27/reproduce_workflow_findings.py
```

The reproduction scripts assert the observed defects and print evidence; a zero exit code means the defects were reproduced, not that the implementation passed a conformance test. They only create temporary test instances. JSON evidence is saved alongside this report; scripts also write their fresh output under `/tmp`.

Prioritize team pairing and full Dutch candidate selection, then result validation and round-state controls. Use established reference pairings/checkers as acceptance tests for any replacement or correction. This review establishes non-compliance with specific requirements; it is not an exhaustive implementation or certification of the official systems.
