import polars as pl
import src.utils as utils


def test_handicap_calculation():
    df = pl.DataFrame(
        {
            "player_rank": ["1k", "1k", "1k", "4k"],
            "opponent_rank": ["1k", "1d", "5d", "15k"]
        }
    )
    df = utils.calculate_nominal_handicap(df,
        "player_rank", 
        "opponent_rank")
    assert df["nominal_handicap"].to_list() == [0, 1, 5, 9]
    assert df["nominal_color"].to_list() == [None, "b", "b", "w"]