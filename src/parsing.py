import polars as pl
import re
import numpy as np

# Matches a string like "35+/b0" and extracts 35 as position, + as result, b as color and 0 as handicap
game_result_pattern_string = r"^0?\?$|^0=0$|^(?P<opponent_position>\d+)?(?P<result>[+\-=])([\/!]?(?P<color>[bwBW])?H?(?P<explicit_handicap>[0-9O]+)?)?$"

# Matches a string like "something |123" and extracts the number 123
pin_pattern = r"\|(?P<pin>\d+)\D*$"
pin_pattern_re = re.compile(pin_pattern)

def parse_pin(s: str):
    match = re.search(pin_pattern_re, s)
    if match:
        return int(match.group("pin"))
    return None

def check_all(df, column_num, pattern):
    result = df.select(
        pl.col(df.columns[column_num]).str.contains(pattern).alias("valid")
    ).select(
        pl.col("valid").all()
    )
    return result["valid"][0]

def parse_gotha_games(gotha_string: str, tournament_id: str|None = None) -> pl.DataFrame:
    # Extract lines that aren't empty, aren't whitespace when name is supposed to be found, not comments and have a pin. Properly 
    # formatted ones should just be not comments, ie. start with ";", but other requirements are needed because data is borked.
    line_list = gotha_string.split("\n")
    line_list = list(filter(lambda x: len(x.strip()) > 20 and not x[5:20].strip() == '' and not x.strip() == '' and not x.strip().startswith(";"), line_list))
    
    # Pins aren't always present, so we extract what we can, and focus on things that are always present.
    pins = map(parse_pin, line_list)
    line_list = list(map(lambda x: x.split("|")[0].strip(), line_list))
    
    lines_split = list(map(lambda x: x.split(), line_list))
    # Some result have seeming random extra characters that are hard to distinguish from real columns.
    # But, since they're always at the end, before pin which we removed, we can just remove extra noise from the end,
    # getting rid of the troublemaker ghost columns
    min_cols = min(map(len, lines_split))
    lines_split = map(lambda x: x[:min_cols], lines_split)
    df = pl.DataFrame(lines_split)
    pins = pl.Series(pins).cast(pl.Int64).alias("pin")
    df = df.with_columns(pins)
    placements = df.get_column(df.columns[0]).alias("placement").cast(pl.Int32)
    surnames = df.get_column(df.columns[1]).alias("surname")
    first_names = df.get_column(df.columns[2]).alias("first_name")
    rank_strings = df.get_column(df.columns[3]).alias("rank")
    raw_ranks = df.with_columns(pl.col(df.columns[3]).str.extract_groups(r"(?P<rank_number>\d+)(?P<dankyu>[pPdDkK])?").alias("raw_rank"))

    raw_ranks = raw_ranks.unnest("raw_rank").with_columns(
        pl.col("rank_number").cast(pl.Int32),
        pl.col("dankyu").fill_null(pl.lit("k")).str.to_lowercase(),
    )
    ranks = raw_ranks.with_columns(
        pl.when(pl.col("dankyu") == "p").then(pl.lit(10))
        .when(pl.col("dankyu") == "d").then(pl.col("rank_number"))
        .when(pl.col("dankyu") == "k").then(1 + pl.col("rank_number") * -1).alias("rank_comparison_number")
        ).get_column("rank_comparison_number")

    games_columns: list[int] = []
    for i in range(2, len(df.columns) - 1):
        is_game_column = check_all(df, i, game_result_pattern_string)
        if is_game_column:
            games_columns.append(i)
    rounds = []
    # For looking up opponent pin.
    opponent_pin_df = pl.DataFrame({"opponent_position": placements, "opponent_pin": pins, "opponent_rank_comparison_number": ranks})
    for i, col in enumerate(games_columns, start=1):
        col_df = df.get_column(df.columns[col]
                            ).str.replace(r"^[+\-=]$", "0="
                            ).str.replace(r"^0=0$", "0="
                            ).str.replace(r"^0?\?$", "0="
                            ).str.extract_groups(game_result_pattern_string
                            ).struct.unnest()
        col_df = col_df.select(
                        pl.col("opponent_position").cast(pl.Int32), 
                        pl.col("result"), 
                        pl.col("color").str.to_lowercase(), 
                        pl.col("explicit_handicap").str.replace("O$", "0").cast(pl.Int32)
                    ).with_columns(
                        pl.lit(i).cast(pl.Int32).alias("round_number"),
                        placements.alias("position"), 
                        pins.alias("pin"),
                        ranks,
                        rank_strings,
                        first_names.alias("first_name"),
                        surnames.alias("surname"),
                    ).join(
                        opponent_pin_df,
                        on=pl.col("opponent_position"), 
                        how="left"
                    ).select([
                        "position", 
                        "pin", 
                        "first_name",
                        "surname",
                        "rank",
                        "rank_comparison_number",
                        "result", 
                        "color", 
                        "opponent_position", 
                        "opponent_pin", 
                        "opponent_rank_comparison_number",
                        "round_number", 
                        "explicit_handicap",
                    ])
        rounds.append(col_df)
    games = pl.concat(rounds)

    return games

def tournament_as_df(gotha_string: str, tournament_id: str|None) -> tuple[pl.DataFrame, pl.DataFrame]:
    if not gotha_string:
        return pl.DataFrame(), pl.DataFrame()
    games = parse_gotha_games(gotha_string)
    info_df = tournament_info(gotha_string).with_columns(
        pl.lit(tournament_id, dtype=pl.String).alias("tournament")
    )
    games = games.with_columns(
        pl.lit(tournament_id, dtype=pl.String).alias("tournament"),
        info_df["gor_weight"].cast(pl.Float64).alias("gor_weight"),
    )
    if "HA" in info_df.columns:
        handicap_reduction = info_df["HA"].str.extract(r"(\d+)").cast(pl.Int32).alias("handicap_reduction")
        games = games.with_columns(
            pl.when((pl.col("explicit_handicap").is_null()) & (pl.col("opponent_position") != 0))
              .then(pl.max_horizontal(
                  pl.min_horizontal(np.absolute(pl.col("rank_comparison_number") - pl.col("opponent_rank_comparison_number")), pl.lit(9)) - handicap_reduction, pl.lit(0)))
              .otherwise(pl.col("explicit_handicap")).alias("handicap")
        )
    else:
        games = games.with_columns(
            pl.col("explicit_handicap").alias("handicap")
        )
    return info_df, games

# Metadata in patterns of "<something>XY[Z]", where X and Y are characters(two characters in total) and Z is a value(arbitrary length)
# Example "; CL[A]", "; KM[6.5]", "; HA[h9]"
metadata_pattern = r"[\s;]*(?P<key>[A-Z]{2})\[(?P<value>[^\]]+)\].*"

def tournament_info(gotha_string: str) -> pl.DataFrame:
    line_list = gotha_string.split("\n")
    metadata_df = pl.DataFrame({
        "line": line_list
    }).with_columns(
        pl.col("line").str.extract_groups(metadata_pattern)
    )["line"].struct.unnest().filter(pl.col("key").is_not_null())
    metadata_df = metadata_df.with_columns(pl.col("key").str.to_uppercase())
    metadata_df = metadata_df.transpose(column_names="key")
    if "CL" in metadata_df.columns:
        metadata_df = metadata_df.with_columns(
            pl.col("CL").str.to_uppercase().alias("CL")
        )
    else:
        metadata_df = metadata_df.with_columns(
            pl.lit(None).alias("CL")
        )
    metadata_df = metadata_df.with_columns(
        pl.when(pl.col("CL") == "A")
        .then(1.0)
        .when(pl.col("CL") == "B")
        .then(0.75)
        .when(pl.col("CL") == "C")
        .then(0.5)
        .when(pl.col("CL") == "D")
        .then(0.25)
        .otherwise(0)
        .cast(pl.Float64)
        .alias("gor_weight"),
    )
    return metadata_df