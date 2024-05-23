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
    make it easy to calculate handicap. Zero is at 1k, higher numbers are stronger.
    
    Pro ranks are set to None, as there is no clear way to calculate handicap for them."""
    return df.with_columns(
        rank_comparison_number_expression(rank_column).alias(output_column)
    )

def rank_comparison_number_expression(rank_column: str, *, anchor_1k_value = 0) -> pl.Expr:
    return (
        pl.when(pl.col(rank_column).str.ends_with("p")).then(pl.lit(None, dtype=pl.Int8))
        .when(pl.col(rank_column).str.ends_with("d")).then(pl.col(rank_column).str.head(-1).cast(pl.Int8) + anchor_1k_value)
        .when(pl.col(rank_column).str.ends_with("k")).then(1 + (pl.col(rank_column).str.head(-1).cast(pl.Int8) * -1) + anchor_1k_value)
    )

def rank_to_nominal_gor_expression(rank_column: str) -> pl.Expr:
    """
    An expression taking rank column, and producing new column `nominal_gor` containing
    nominal gor rating as per EGD system.
    
    * Kyu ranks go from 30k -> -900 to 1k -> 2000(100 increment),
    * Dan ranks go from 1d -> 2100 to 6d -> 2700(100 increment),
    * Pro ranks go from 1p -> 2700 to 9p -> 2940(30 increment)
    
    Ranks capped from below by -900, so 30k and below are -900."""
    return pl.max_horizontal(
        pl.when(pl.col(rank_column).str.ends_with("k"))
            .then(100 * (21 - pl.col(rank_column).str.head(-1).cast(pl.Int32)))
            
            .when(pl.col(rank_column).str.ends_with("d"))
            .then(100 * (20+pl.col(rank_column).str.head(-1).cast(pl.Int32)))
            
            .when(pl.col(rank_column).str.ends_with("p"))
            .then(2670 + (30 * pl.col(rank_column).str.head(-1).cast(pl.Int32)))
            
            .otherwise(pl.lit(None, dtype=pl.Int32)),
        pl.lit(-900)
    ).alias("nominal_gor")