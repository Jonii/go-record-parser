import polars as pl

def calculate_nominal_handicap(
    df: pl.DataFrame,
    player_rank_column: str,
    opponent_rank_column: str,
) -> pl.DataFrame:
    """
    Calculates nominal handicap given ranks, and color for the first player.

    Same rank players have nominal handicap of 0, and there is no preferred color, so it is None.
    2d playing against 1k would give two handicap stones, and have white color.
    15k playing against 1k would get 9 handicap stones, and have black color.
    Results are in columns "nominal_handicap" and "nominal_color".
    """
    rank_comparison_column = "rank_comparison_number"
    opponent_rank_comparison_column = "opponent_rank_comparison_number"
    df = _calculate_rank_comparison_number(df, player_rank_column, rank_comparison_column)
    df = _calculate_rank_comparison_number(df, opponent_rank_column, opponent_rank_comparison_column)
    
    nominal_handicap_column = "nominal_handicap"
    nominal_color_column = "nominal_color"
    nominal_handicap_signed_column = "nominal_handicap_signed"
    df = df.with_columns(
        (pl.col(rank_comparison_column) - pl.col(opponent_rank_comparison_column)).alias(nominal_handicap_signed_column)
    ).with_columns(
        pl.when(pl.col(nominal_handicap_signed_column) > 0)
            .then(pl.col(nominal_handicap_signed_column))
            .otherwise(-1 * pl.col(nominal_handicap_signed_column)).alias(nominal_handicap_column),
        pl.when(pl.col(nominal_handicap_signed_column) > 0).then(pl.lit("w"))
            .when(pl.col(nominal_handicap_signed_column) < 0).then(pl.lit("b"))
            .otherwise(pl.lit(None)).alias(nominal_color_column),
    ).with_columns(
        pl.when(pl.col(nominal_handicap_column) > 9).then(pl.lit(9))
            .otherwise(pl.col(nominal_handicap_column)).cast(pl.Int8).alias(nominal_handicap_column)
    )
    return df.select(pl.all().exclude([rank_comparison_column, opponent_rank_comparison_column, nominal_handicap_signed_column]))

def _calculate_rank_comparison_number(
        df: pl.DataFrame,
        rank_column: str = "rank",
        output_column: str = "rank_comparison_number"
    ):
    """
    Adds rank comparison number column, an arbitrary number to 
    make it easy to calculate handicap. Zero is at 1k, higher numbers are stronger."""
    rank_extractor_pattern = r"(?P<rank_number>\d+)(?P<dankyu>[pdk])?"
    df = df.with_columns(
        pl.col(rank_column).str.to_lowercase(),
    ).with_columns(
        pl.col(rank_column).str.extract_groups(rank_extractor_pattern).alias("rank_struct"),
    ).unnest("rank_struct").with_columns(
        pl.col("rank_number").cast(pl.Int8)
    ).with_columns(
        pl.when(pl.col("dankyu") == "p").then(pl.lit(None, dtype=pl.Int8))
        .when(pl.col("dankyu") == "d").then(pl.col("rank_number"))
        .when(pl.col("dankyu") == "k").then(1 + pl.col("rank_number") * -1).alias(output_column),
    ).select(pl.all().exclude(["rank_struct", "rank_number", "dankyu"]))

    return df