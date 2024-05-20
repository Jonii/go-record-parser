import polars as pl
import src.gor_calculator as gc

sample_data = {
    "Round": [1, 2, 3, 4, 5],
    "Gor": [2705.84]*5,
    "Adjusted_GoR": [2705.840]*5,
    "Con": [5.709]*5,
    "Beta": [-44.710]*5,
    "Bonus": [0.001]*5,
    "SE": [0.722, 0.726, 0.414, 0.401, 0.745],
    "GoR_Change": [1.588, 1.566, 3.347, -2.287, -4.252],
    "GoR_Change_Alt": [-4.122, -4.144, -2.363, 3.422, 1.458],
    "Color": ["w"]*5,
    "Opponent_Color": ["b"]*5,
    "Handicap": [4, 5, 6, 6, 6],
    "Result": ["+", "+", "+", "-", "-"],
    "Tournament_Weight": [1]*5,
    "Opponent_Rank": ["3d", "2d", "2d", "2d", "1d"],
    "Opponent_GoR": [2268.985, 2167.087, 2184.591, 2188.993, 2057.534],
    "Opponent_Adjusted_GoR": [2618.985, 2617.087, 2734.591, 2738.993, 2607.534],
    "Opponent_Con": [13.790, 16.035, 15.640, 15.542, 18.587],
    "Opponent_Beta": [-45.665, -45.685, -44.363, -44.308, -45.782],
    "Opponent_Bonus": [0.181, 0.367, 0.331, 0.322, 0.616],
    "Opponent_SE": [0.278, 0.274, 0.586, 0.599, 0.255],
    "Opponent_GoR_Change": [-3.651, -4.026, -8.833, 6.552, 14.462]
}


def test_gor_calculation():
    df = pl.DataFrame(sample_data)
    gor_expr = gc.gor_change_expression(
        rating_volatility_column=gc.rating_volatility_expression("Gor"),
        win_column="Result",
        expected_result_column=gc.expected_result_expression(
            beta_column=gc.beta_expression(
                gc.adjusted_gor_expression(
                    gor_column="Gor",
                    handicap_column="Handicap",
                    color_column="Color",
                    output_column="Adjusted_GoR"
                ),
                output_column="Beta"
            ),
            opponent_beta_column=gc.beta_expression(
                gc.adjusted_gor_expression(
                    gor_column="Opponent_GoR",
                    handicap_column="Handicap",
                    color_column=gc.swap_color_expression("Color"),
                    output_column="Opponent_Adjusted_GoR"
                ),
                output_column="Opponent_Beta"
            )
        ),
        bonus_column=gc.bonus_expression("Gor"),
        gor_weight_column="Tournament_Weight",
        output_column="GoR_Change_Computed"
    )
    diff_df = df.select(
        gor_expr,
        "GoR_Change",
    ).with_columns(
        (pl.col("GoR_Change") - pl.col("GoR_Change_Computed")).round(3).alias("Diff")
    )
    assert diff_df["Diff"].abs().max() == 0.0 # type: ignore


def test_gor_calculation_function():
    df = pl.DataFrame(sample_data)
    df = gc.calculate_gor_change(
        games_df=df,
        gor_column="Gor",
        gor_opponent_column="Opponent_GoR",
        handicap_column="Handicap",
        color_column="Color",
        result_column="Result",
        gor_weight_column="Tournament_Weight",
        output_column="GoR_Change_Computed"
    )

    diff_df = df.select(
        "GoR_Change",
        "GoR_Change_Computed"
    ).with_columns(
        (pl.col("GoR_Change") - pl.col("GoR_Change_Computed")).round(3).alias("Diff")
    )
    assert diff_df["Diff"].abs().max() == 0.0 # type: ignore

sample_data2 = {
    "Round": [1, 2, 3, 4, 5],
    "Gor": [2705.84]*5,
    "Adjusted_GoR": [2705.840]*5,
    "Con": [5.709]*5,
    "Beta": [-44.710]*5,
    "Bonus": [0.001]*5,
    "SE": [0.722, 0.726, 0.414, 0.401, 0.745],
    "GoR_Change": [1.588, 1.566, 3.347, -2.287, -4.252],
    "GoR_Change_Alt": [-4.122, -4.144, -2.363, 3.422, 1.458],
    "Color": ["w"]*5,
    "Opponent_Color": ["b"]*5,
    "Handicap": [4, 5, 6, 6, 6],
    "Result": ["+", "+", "+", "-", "-"],
    "Tournament_Weight": [1]*5,
    "Opponent_Rank": ["3d", "2d", "2d", "2d", "1d"],
    "Opponent_GoR": [2268.985, 2167.087, 2184.591, 2188.993, 2057.534],
    "Opponent_Adjusted_GoR": [2618.985, 2617.087, 2734.591, 2738.993, 2607.534],
    "Opponent_Con": [13.790, 16.035, 15.640, 15.542, 18.587],
    "Opponent_Beta": [-45.665, -45.685, -44.363, -44.308, -45.782],
    "Opponent_Bonus": [0.181, 0.367, 0.331, 0.322, 0.616],
    "Opponent_SE": [0.278, 0.274, 0.586, 0.599, 0.255],
    "Opponent_GoR_Change": [-3.651, -4.026, -8.833, 6.552, 14.462]
}


# Raw data: 
# gor, gor_change, color, handicap, result, opponent_gor
# 1800.000	5.845	b	0 explicit	=	2028.143
sample_data_jigo = {
    "gor": [1800.000],
    "gor_change": [5.845],
    "color": ["b"],
    "handicap": [0],
    "result": ["="],
    "opponent_gor": [2028.143],
    "tournament_weight": [0.75],
}

def test_jigo_calculation():
    df = pl.DataFrame(sample_data_jigo)
    df = gc.calculate_gor_change(
        games_df=df,
        gor_column="gor",
        gor_opponent_column="opponent_gor",
        handicap_column="handicap",
        color_column="color",
        result_column="result",
        gor_weight_column="tournament_weight",
        output_column="gor_change_computed",
    )

    diff_df = df.select(
        "gor_change",
        "gor_change_computed"
    ).with_columns(
        (pl.col("gor_change") - pl.col("gor_change_computed")).round(3).alias("Diff")
    )
    assert diff_df["Diff"].abs().max() == 0.0 # type: ignore