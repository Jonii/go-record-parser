import polars as pl
import src.utils as utils


def test_handicap_calculation():
    df = pl.DataFrame(
        {
            "player_rank": ["1k", "1k", "1k", "4k", "5k"],
            "opponent_rank": ["1k", "1d", "5d", "15k", None]
        }
    )
    df = utils.calculate_nominal_handicap(df,
        "player_rank", 
        "opponent_rank")
    assert df["nominal_handicap"].to_list() == [0, 1, 5, 9, None]
    assert df["nominal_color"].to_list() == [None, "b", "b", "w", None]

def test_rank_comparison_number_expression():
    df = pl.DataFrame(
        {
            "rank": ["1k", "1d", "5d", "15k", "1p", "30k"]
        }
    )
    df = df.with_columns(utils.rank_comparison_number_expression("rank", anchor_1k_value=0).alias("rank_comparison_number"))
    assert df["rank_comparison_number"].to_list() == [0, 1, 5, -14, None, -29]
    df = df.with_columns(utils.rank_comparison_number_expression("rank", anchor_1k_value=30).alias("rank_comparison_number_anchored_at_31k"))
    assert df["rank_comparison_number_anchored_at_31k"].to_list() == [30, 31, 35, 16, None, 1]

def test_nominal_gor_calculation():
    kyu_ranks = [
        ("40k", -900),
        ("31k", -900), 
        ("30k", -900), 
        ("29k", -800), 
        ("28k", -700), 
        ("27k", -600), 
        ("26k", -500), 
        ("25k", -400), 
        ("24k", -300), 
        ("23k", -200), 
        ("22k", -100), 
        ("21k", 0), 
        ("20k", 100), 
        ("19k", 200), 
        ("18k", 300), 
        ("17k", 400), 
        ("16k", 500), 
        ("15k", 600), 
        ("14k", 700), 
        ("13k", 800), 
        ("12k", 900), 
        ("11k", 1000), 
        ("10k", 1100), 
        ("9k", 1200), 
        ("8k", 1300), 
        ("7k", 1400), 
        ("6k", 1500), 
        ("5k", 1600), 
        ("4k", 1700), 
        ("3k", 1800), 
        ("2k", 1900), 
        ("1k", 2000)
    ]

    dan_ranks = [
        ("1d", 2100), 
        ("2d", 2200), 
        ("3d", 2300), 
        ("4d", 2400), 
        ("5d", 2500), 
        ("6d", 2600)
    ]
    pro_ranks = [
        ("1p", 2700), 
        ("2p", 2730), 
        ("3p", 2760), 
        ("4p", 2790), 
        ("5p", 2820), 
        ("6p", 2850), 
        ("7p", 2880), 
        ("8p", 2910), 
        ("9p", 2940)
    ]

    df = pl.DataFrame(
        {
            "rank": [r[0] for r in kyu_ranks + dan_ranks + pro_ranks]
        }
    )
    df = df.with_columns(utils.rank_to_nominal_gor_expression("rank").alias("nominal_gor"))
    assert df["nominal_gor"].to_list() == [r[1] for r in kyu_ranks + dan_ranks + pro_ranks]