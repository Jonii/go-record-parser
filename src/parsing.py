import polars as pl
import polars.selectors as cs
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
    raw_df = pl.DataFrame(lines_split)
    pins = pl.Series(pins).cast(pl.Int64).alias("pin")
    raw_df = raw_df.with_columns(pins)

    df = raw_df.select([
        pl.lit(tournament_id, dtype=pl.String).alias("tournament"),
        pl.col(raw_df.columns[0]).alias("position").cast(pl.Int32),
        pins,
        pl.col(raw_df.columns[1]).alias("surname"),
        pl.col(raw_df.columns[2]).alias("first_name"),
        pl.col(raw_df.columns[3]).alias("rank"),
    ])

    # Adds rank comparison number column, an arbitrary number to 
    # make it easy to calculate handicap. Zero is at 1k, higher numbers are stronger.
    rank_extractor_pattern = r"(?P<rank_number>\d+)(?P<dankyu>[pPdDkK])?"
    df = df.with_columns(
        pl.col("rank").str.extract_groups(rank_extractor_pattern).alias("rank_struct"),
    ).unnest("rank_struct").with_columns(
        pl.col("rank_number").cast(pl.Int8),
        pl.col("dankyu").str.to_lowercase()
    ).with_columns(
        pl.when(pl.col("dankyu") == "p").then(pl.lit(10))
        .when(pl.col("dankyu") == "d").then(pl.col("rank_number"))
        .when(pl.col("dankyu") == "k").then(1 + pl.col("rank_number") * -1).alias("rank_comparison_number")
    )

    # Melt the dataframe so each game is on its own row, with result at column "raw_result"
    games_df = raw_df.select([column.name for column in
        raw_df.select(pl.all(
            ).exclude("pin", *raw_df.columns[0:3]
            ).str.extract_groups(game_result_pattern_string
            )
        ).select(
            pl.all().struct.field("result").is_not_null().all().name.keep()
        )
        if column.item() == True]
    )
    games_df = games_df.rename({old_name: f"{i}" for i, old_name in enumerate(games_df.columns, start=1)})
    games_df = pl.concat([df, games_df], how="horizontal"
        ).melt(id_vars=df.columns, value_vars=cs.matches("round_.*"), variable_name="round_number", value_name="raw_result"
        ).with_columns(pl.col("round_number").cast(pl.Int8)
    )


    # Parse the raw result column into structured columns.
    # Lots of special cases with result strings, some records have results that are simply "?",
    # some have results that are "0=0", some have "0", some have "+" or "-", etc, which are entirely useless
    # for gor calculations as there's no way to determine the opponent. This treats all of those as free rounds.
    games_df = games_df.with_columns(
        pl.col("raw_result")
            .str.replace(r"^[+\-=]$", "0=")
            .str.replace(r"^0=0$", "0=")
            .str.replace(r"^0?\?$", "0=")
            .str.extract_groups(game_result_pattern_string)
            .alias("result_struct")
    ).unnest("result_struct").with_columns(
        pl.col("opponent_position").cast(pl.Int32),
        pl.col("color").str.to_lowercase(),
        pl.col("explicit_handicap").str.replace("O$", "0").cast(pl.Int32)
    # Add info about opponent to the dataframe, with prefix "opponent_". Polars doesn't have "prefix", hence "rename" call.
    ).join(
        df, 
        left_on="opponent_position", 
        right_on="position", 
        how="left", 
        suffix="_opponent"
    ).rename(lambda x: "opponent_" + x.replace("_opponent", "") if x.endswith("_opponent") else x
    ).select([
        "tournament",
        "position",
        "pin", 
        "surname", 
        "first_name", 
        "rank", 
        "rank_comparison_number",
        "opponent_position", 
        "opponent_pin",
        "opponent_surname",
        "opponent_first_name",
        "opponent_rank",
        "opponent_rank_comparison_number",
        "round_number",
        "result",
        "color",
        "explicit_handicap",
    ])
    return games_df

def tournament_as_df(gotha_string: str, tournament_id: str|None) -> tuple[pl.DataFrame, pl.DataFrame]:
    '''
    Parse tournament from gotha string and tournament id string.

    Returns two dataframes, one with tournament metadata and one with games.

    Tournament metadata columns:
    - tournament_id: str
    - CL: str
    - gor_weight: float
    - Any other metadata found in the gotha string.

    Games columns:
    - tournament: str
    - position: int
    - pin: int
    - surname: str
    - first_name: str
    - rank: str
    - rank_comparison_number: int
    - opponent_position: int
    - opponent_pin: int
    - opponent_surname: str
    - opponent_first_name: str
    - opponent_rank: str
    - opponent_rank_comparison_number: int
    - result: str
    - color: str
    - round_number: int
    - explicit_handicap: int
    - handicap: int
    - gor_weight: float

    Gor weight is based on tournament class(A, B, C, D), for gor calculations.

    Handicap is primarily taken from explicit handicap column, or if one is not present, one is calculated
    based on handicap policy if present. If neither is present, handicap is set to None.

    Explicit handicap is taken from game result column, where the last, optional digit is the handicap.

    Handicap policy is taken from the gotha string, where it is in the format "HA[h9]". For example, h5 would mean, handicap used is
    rank difference - 5. Alternate handicap formats exist, but are not supported. EGD data has explicit handicap for all records
    with alternate formats, so in practice this shouldn't be a problem.

    Rank comparison number maps ranks to numbers, where 1d is 1, 1k is 0, 9d is 9, 10k is -9, etc. Professional ranks 1-9p are static 10.
    Pro ratings don't really have a mapping to amateur ranks though. Handicap tournaments with pro players entering with their pro ranks
    with implicit handicaps would likely result in wrong handicaps. This doesn't seem to be solvable from the available data.
    '''

    if not gotha_string:
        return pl.DataFrame(), pl.DataFrame()
    games = parse_gotha_games(gotha_string)
    info_df = tournament_info(gotha_string).with_columns(
        pl.lit(tournament_id, dtype=pl.String).alias("tournament")
    )
    if "HA" in info_df.columns:
        handicap_reduction = info_df["HA"].str.extract(r"(\d+)").cast(pl.Int32).alias("handicap_reduction")
        games = games.with_columns(
            pl.when((pl.col("explicit_handicap").is_null()) & (pl.col("opponent_rank_comparison_number").is_not_null()))
              .then(pl.max_horizontal(
                  pl.min_horizontal(np.absolute(pl.col("rank_comparison_number") - pl.col("opponent_rank_comparison_number")), pl.lit(9)) - handicap_reduction, pl.lit(0)))
              .otherwise(pl.col("explicit_handicap")).alias("handicap")
        )
    else:
        games = games.with_columns(
            pl.col("explicit_handicap").alias("handicap")
        )
    games = games.with_columns(
        pl.lit(tournament_id, dtype=pl.String).alias("tournament"),
        info_df["gor_weight"].cast(pl.Float64).alias("gor_weight"),
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