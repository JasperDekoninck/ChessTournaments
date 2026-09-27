"""Translate our tournament records into Gacrux's FIDE 2026 pairing model.

Keep every entrant and past result, including withdrawn players and forfeits: the
engine needs those records to reconstruct scores, floats, colours and bye rights.
Only `present` controls eligibility for the round being paired.
"""
from decimal import Decimal

from gacrux.gacruxexeptions import GacruxNoLegalPairing
from gacrux.pairingdutch import pairing_dutch
from gacrux.pairingfideteam import pairing_fideteam


# Explicit played flags matter: a forfeit awards points but neither a colour nor
# an opponent for the no-rematch rule. A full-point unplayed win also bars a bye.
ENGINE_RESULTS = {
    "1-0": ("W", "L", True),
    "0-1": ("L", "W", True),
    "1/2-1/2": ("D", "D", True),
    "1F-0F": ("W", "Z", False),
    "0F-1F": ("Z", "W", False),
    "0F-0F": ("Z", "Z", False),
}


def pair_swiss(entries, history, available_ids, round_no, rounds_planned, *, is_team=False):
    """Return a complete set of boards, or [] if no legal full pairing exists."""
    if not available_ids or not 1 <= round_no <= rounds_planned:
        return []
    ordered = sorted(entries, key=lambda row: (
        -int(row["seed_rating"]), row["imported_name"].casefold(), row["id"],
    ))
    # Dense starting numbers are required by the engine. Database ids are not
    # starting numbers, and may have gaps or belong to other tournaments.
    numbers = {row["id"]: number for number, row in enumerate(ordered, 1)}
    entry_ids = {number: entry_id for entry_id, number in numbers.items()}
    scores = {
        "W": Decimal("1"), "D": Decimal("0.5"), "L": Decimal("0"),
        "Z": Decimal("0"), "P": "W", "A": "D", "U": "Z",
    }
    tournament = {
        "tournamentType": "Team-Swiss" if is_team else "Swiss",
        "numRounds": rounds_planned,
        "currentRound": round_no - 1,
        "teamTournament": is_team,
        "topColor": "w",
        "pairingSystem": ["fideteam", "mp"] if is_team else ["dutch"],
        "rankOrder": ["PTS"],
        "scoreSystem": {"game": scores},
        "competitors": [],
        "gameList": [],
        "matchList": [],
    }
    if is_team:
        # This app records one result per team encounter (1 / 0.5 / 0), not
        # individual board scores. Use match points only, Type A colours, and
        # preserve its full-point bye convention; do not invent board scores.
        tournament["teamSize"] = 1
        tournament["scoreSystem"].update({
            "primary": "match",
            "match": {**scores, "FG": "W*", "HG": "D*", "ZG": "Z*", "PG": "W*"},
        })
    for entry in ordered:
        number = numbers[entry["id"]]
        competitor = {
            "cid": number, "rank": number, "random": number,
            "present": entry["id"] in available_ids,
            "rating": {"rating": int(entry["seed_rating"])},
        }
        if is_team:
            competitor.update({"teamId": number, "cplayers": []})
        tournament["competitors"].append(competitor)

    records = tournament["matchList" if is_team else "gameList"]
    for pairing in history:
        if pairing["round_no"] >= round_no:
            continue
        white = numbers[pairing["white_entry_id"]]
        black = numbers.get(pairing["black_entry_id"], 0)
        code = pairing["result_code"]
        if not black:
            if code != "BYE":
                raise ValueError("A previous bye has no valid result.")
            wresult, bresult, played = "P", "Z", False
        elif code in ENGINE_RESULTS:
            wresult, bresult, played = ENGINE_RESULTS[code]
        else:
            raise ValueError("Finish all previous games with valid results before pairing the next round.")
        records.append({
            "id": len(records) + 1,
            "round": pairing["round_no"], "board": pairing["board_no"],
            "white": {"cid": white, "result": wresult},
            "black": {"cid": black, "result": bresult} if black else None,
            "played": played, "rated": False, "games": [],
        })

    engine_class = pairing_fideteam if is_team else pairing_dutch
    engine = engine_class(tournament, round_no, {"experimental": [], "verbose": 0})
    try:
        brackets = engine.compute_pairing(False)
    except GacruxNoLegalPairing:
        return []

    def board_order(pair):
        # General Handling 3.6: use the TPN of the higher-ranked player,
        # not the smallest TPN of either player in a mixed-score pairing.
        white, black = pair["w"], pair["b"]
        if not black:
            return (1, 0, 0, white)
        ws = engine.competitors[white]["pts"]
        bs = engine.competitors[black]["pts"]
        higher = white if ws > bs or (ws == bs and white < black) else black
        return (0, -max(ws, bs), -(ws + bs), higher)

    pairs = sorted(
        (pair for bracket in brackets for pair in bracket["pairs"]),
        key=board_order,
    )
    boards = [
        {"board_no": index, "white_entry_id": entry_ids[pair["w"]],
         "black_entry_id": entry_ids.get(pair["b"])}
        for index, pair in enumerate(pairs, 1)
    ]
    seated = [entry_id for board in boards for entry_id in (
        board["white_entry_id"], board["black_entry_id"],
    ) if entry_id is not None]
    # Never persist partial/duplicate output, including an unpairable position.
    if len(seated) != len(available_ids) or set(seated) != set(available_ids):
        return []
    return boards
