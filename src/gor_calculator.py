import numpy as np
import polars as pl

def column_name_to_expr(column_name: str|pl.Expr) -> pl.Expr:
    return pl.col(column_name) if isinstance(column_name, str) else column_name

def swap_color_expression(color_column: str = "color") -> pl.Expr:
    """
    Returns a Polars expression to swap colors.
    """
    return pl.when(pl.col(color_column) == "w").then(pl.lit("b")).when(pl.col(color_column) == "b").then(pl.lit("w")).otherwise(pl.lit(None))

def adjusted_gor_expression(gor_column: str|pl.Expr, handicap_column: str|pl.Expr, color_column: str|pl.Expr = "color", output_column: str = "adjusted_gor",) -> pl.Expr:
    """
    Returns a Polars expression to calculate adjusted GOR based on color and handicap.
    """
    color_column = column_name_to_expr(color_column)
    handicap_column = column_name_to_expr(handicap_column)
    gor_column = column_name_to_expr(gor_column)
    return (
        pl.when((color_column == "b") & (handicap_column > 0))
        .then(gor_column + (handicap_column * 100 - 50))
        .otherwise(gor_column)
        .alias(output_column)
    )

def beta_expression(adjusted_gor_column: str|pl.Expr, output_column: str) -> pl.Expr:
    """
    Returns a Polars expression to calculate beta values from adjusted GORs.
    """
    adjusted_gor_column = column_name_to_expr(adjusted_gor_column)
    return (np.log(3300 - adjusted_gor_column) * -7).alias(output_column)

    #return (np.log(3300 - pl.col(adjusted_gor_column)) * -7).alias(output_column)
    #

def expected_result_expression(beta_column: str|pl.Expr = "beta",
                               opponent_beta_column: str|pl.Expr = "beta_opponent",
                               output_column: str = "expected_result") -> pl.Expr:
    """
    Returns a Polars expression to calculate the expected result, based on betas.

    The expected result is the probability of the first player winning the game(1.0 = 100% chance of winning, 0.0 = 0% chance of winning)
    """
    beta_column = column_name_to_expr(beta_column)
    opponent_beta_column = column_name_to_expr(opponent_beta_column)
    expected_result = (1 / (1 + np.exp(opponent_beta_column - beta_column))).alias(output_column)
    #expected_result = (1 / (1 + np.exp(pl.col(opponent_beta_column) - pl.col(beta_column)))).alias(output_column)
    return expected_result


def rating_volatility_expression(gor_column: str|pl.Expr = "igor",
                                 output_column: str = "rating_volatility") -> pl.Expr:
    """
    Returns a Polars expression to compute rating volatility based on gor.

    Rating volatility acts as a multiplier for Gor change, lower rated players experience swifter rating changes.
    The rationale there is roughly, lower rated players can change in their rank more quickly, so
    the rating system needs to be able to keep up with that.
    """
    gor_column = column_name_to_expr(gor_column)
    rating_volatility = (np.power(((3300 - gor_column) / 200), 1.6)).alias(output_column) # type: ignore
    #rating_volatility = (np.power(((3300 - pl.col(gor_column)) / 200), 1.6)).alias(output_column)  # type: ignore
    return rating_volatility


def bonus_expression(gor_column: str|pl.Expr = "igor",
                     output_column: str = "bonus") -> pl.Expr:
    """
    Returns a Polars expression to calculate bonuses based on GOR.

    Bonuses are essentially "How much player probably improved from playing a game",
    which is then added to the Gor change. Lower rated players are expected to learn more,
    so their bonus is higher.
    """
    gor_column = column_name_to_expr(gor_column)
    bonus = (np.log(1 + np.exp((2300 - gor_column) / 80)) / 5).alias(output_column)
    return bonus


def gor_change_expression(rating_volatility_column: str|pl.Expr = "rating_volatility",
                           win_column: str|pl.Expr = "result",
                           expected_result_column: str|pl.Expr = "expected_result",
                           bonus_column: str|pl.Expr = "bonus",
                           gor_weight_column: str|pl.Expr = "gor_weight",
                           output_column: str = "gor_change") -> pl.Expr:
    """
    Returns Polars expression to calculate Gor change based on rating volatility, win, expected result, bonus, and Gor weight.
    """
    rating_volatility_column = column_name_to_expr(rating_volatility_column)
    win_column = column_name_to_expr(win_column)
    expected_result_column = column_name_to_expr(expected_result_column)
    bonus_column = column_name_to_expr(bonus_column)
    gor_weight_column = column_name_to_expr(gor_weight_column)

    gor_change_raw = (
        rating_volatility_column * (pl.when(win_column == "+").then(1).when(win_column == "-").then(0).otherwise(pl.lit(None)) - expected_result_column) + bonus_column
    ).alias("Raw_GoR_Change")
    
    gor_change = (gor_change_raw * gor_weight_column).alias(output_column)

    #gor_change_raw = (
    #    pl.col(rating_volatility_column) * (pl.when(pl.col(win_column) == "+").then(1).otherwise(0) - pl.col(expected_result_column)) + pl.col(bonus_column)
    #).alias(output_column_raw)
    
    #gor_change = (pl.col(output_column_raw) * pl.col(gor_weight_column)).alias(output_column)
    
    return gor_change


def add_gors(games_df: pl.DataFrame, all_events_df: pl.DataFrame):
    return games_df.join(
        all_events_df.select(["pin", "igor", "fgor", "tournament", "grade"]), on=["pin", "tournament"]
    ).join(
        all_events_df.select(["pin", "igor", "fgor", "tournament", "grade"]), left_on=["opponent_pin", "tournament"], right_on=["pin", "tournament"], suffix="_opponent"
    )

def calculate_gor_change(
        games_df: pl.DataFrame,
        gor_column: str = "igor",
        gor_opponent_column: str = "igor_opponent",
        handicap_column: str = "handicap",
        color_column: str = "color",
        result_column: str = "result",
        gor_weight_column: str = "tournament_weight",
        output_column: str = "gor_change",
) -> pl.DataFrame:
    gor_expr = gor_change_expression(
        rating_volatility_column=rating_volatility_expression(gor_column),
        win_column=result_column,
        expected_result_column=expected_result_expression(
            beta_column=beta_expression(
                adjusted_gor_expression(
                    gor_column=gor_column,
                    handicap_column=handicap_column,
                    color_column=color_column,
                    output_column="adjusted_gor"
                ),
                output_column="beta"
            ),
            opponent_beta_column=beta_expression(
                adjusted_gor_expression(
                    gor_column=gor_opponent_column,
                    handicap_column=handicap_column,
                    color_column=swap_color_expression(color_column),
                    output_column="adjusted_gor_opponent"
                ),
                output_column="beta_opponent"
            )
        ),
        bonus_column=bonus_expression(gor_column),
        gor_weight_column=gor_weight_column,
        output_column=output_column
    )
    games_df = games_df.with_columns(gor_expr)
    return games_df