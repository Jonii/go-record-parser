import src.parsing as parsing


# 
#
# Special cases of note:
# - Marking free round by "0=0" instead of "0=".
# - Adding a note to match by exclamation mark at the end. For this parsing,
#   the note is ignored.
# - Non-result column entry matching result column entry. For example, "0=" might appear
#   in other columns, for example to indicate that SOSOS is tied in McMahon system. These should not
#   be interpreted as a match result.
# - Ranks marked without any ending(k, d, p). These seem to imply kyu ranks.

test_gotha = """
; CL[A]
; EV[Test tournament]
; PC[FI, Helsinki]
; DT[2022-02-22,2022-02-23]
; HA[h9]
; KM[6.5]
; TM[90]
; CM[ Test comment ]
;.
; Test tournament
; 29/03/2023
;
; Pl Name                   Grd Co Club  MMS  SOS SOSO
 1 Voittaja Ykkonen         4d SE  Stock  36   206  1231 0=   5+/w1   4-/w0   3+/b0   2+/w0   5+/b0   4+/b0    |14011111
 2 Jaba Kakkonen            3d NO  Oslo   36   205  1238 0    3+/b0!  5+/b0   4-/b0   1-/w0   4+/b0   5-/b0    |10222222
 3 Tyyppi Kolmonen          4d FI  Heh    35   206  1228 0    2-/w0!  0=0      1-/w0   4-/b0   0=      0=       |10333333
 4 Peluri Nelja             3k ES  Pol    35   205  1224 0    0=      1+/b0   2+/w0   3+/w0   2-/w0   1-/w0    |15444444
 5 Pelaaja Viisi            5 FR  Mel    35   203  1224 0    1-/b1   2-/w0   0=      0=      1-/b0   2+/w0    |14555555
"""

def test_tournament_info():
    t_id = "T000101F"
    info_df, games_df = parsing.tournament_as_df(test_gotha, t_id)
    assert info_df["CL"][0] == "A"
    assert info_df["EV"][0] == "Test tournament"
    assert info_df["PC"][0] == "FI, Helsinki"
    assert info_df["DT"][0] == "2022-02-22,2022-02-23"
    assert info_df["HA"][0] == "h9"
    assert info_df["KM"][0] == "6.5"
    assert info_df["TM"][0] == "90"
    assert info_df["CM"][0] == " Test comment "
    assert info_df["tournament"][0] == t_id

def test_games_columns():
    t_id = "T000101F"
    info_df, games_df = parsing.tournament_as_df(test_gotha, t_id)
    assert games_df.columns == [
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
        "handicap",
        "gor_weight",
    ]

def test_parsing_player_info():
    t_id = "T000101F"
    info_df, games_df = parsing.tournament_as_df(test_gotha, t_id)
    rounds_dfs = games_df.partition_by(["round_number"])
    for rounds_dfs in rounds_dfs:
        rounds_dfs = rounds_dfs.sort("position")
        assert rounds_dfs["position"].to_list() == [1, 2, 3, 4, 5]
        assert rounds_dfs["surname"].to_list() == ["Voittaja", "Jaba", "Tyyppi", "Peluri", "Pelaaja"]
        assert rounds_dfs["first_name"].to_list() == ["Ykkonen", "Kakkonen", "Kolmonen", "Nelja", "Viisi"]
        assert rounds_dfs["rank"].to_list() == ["4d", "3d", "4d", "3k", "5k"]
        assert rounds_dfs["rank_comparison_number"].to_list() == [4, 3, 4, -2, -4]
        assert rounds_dfs["pin"].to_list() == [14011111, 10222222, 10333333, 15444444, 14555555]

def test_parsing_games():
    t_id = "T000101F"
    info_df, games_df = parsing.tournament_as_df(test_gotha, t_id)
    player_dfs = games_df.partition_by(["pin"], as_dict=True)
    first_player_df = player_dfs[(14011111,)].sort("round_number")
    assert first_player_df["result"].to_list() == ["+", "-", "+", "+", "+", "+"]
    assert first_player_df["color"].to_list() == ["w", "w", "b", "w", "b", "b"]
    assert first_player_df["handicap"].to_list() == [1, 0, 0, 0, 0, 0]
    assert first_player_df["opponent_position"].to_list() == [5, 4, 3, 2, 5, 4]

test_gotha2 = """
; CL[B]
; EV[GPE]
; PC[DE, Hamburg]
; KM[5.5]
; TM[0]
; HA[h3]
;                     Tourney title, Hamburg 1996
;                             May 25 - 27, 1996
;
;Pl     Name              Rank  Club   MM S SOS SSOS  1.   2.   3.   4.   5.   6.  
  1 Playah One                3d NL Hilv 29 6 159 952  12+   3+   7+   2+   6+   8+  |11111111
  2 Gamer Two                5d DE Sie  28 5 160 940  10+  16+   5+   1-   4+  3+   |22222222
  3 Fella The_Third_Really_Super_Cool_Dude 1d DE HH 27 4 158 947   4+   1-  16+  11+   12-   2-     |33333333
  4 Woah Dude                4d DE HH 27 4 157 946   3-  11+  10+   6+   2-   5-   |44444444
"""

def test_long_name_parsing():
    t_id = "T000102F"
    info_df, games_df = parsing.tournament_as_df(test_gotha2, t_id)
    player_dfs = games_df.partition_by(["pin"], as_dict=True)
    long_player_df = player_dfs[(33333333,)].sort("round_number")
    assert long_player_df["surname"].to_list() == ["Fella"] * 6
    assert long_player_df["first_name"].to_list() == ["The_Third_Really_Super_Cool_Dude"] * 6
    assert long_player_df["rank"].to_list() == ["1d"] * 6

def test_plain_game_result_parsing():
    t_id = "T000102F"
    info_df, games_df = parsing.tournament_as_df(test_gotha2, t_id)
    player_dfs = games_df.partition_by(["pin"], as_dict=True)
    long_player_df = player_dfs[(33333333,)].sort("round_number")
    assert long_player_df["result"].to_list() == ["+", "-", "+", "+", "-", "-"]
    assert long_player_df["color"].to_list() == [None] * 6
    assert long_player_df["opponent_position"].to_list() == [4, 1, 16, 11, 12, 2]
    assert long_player_df["explicit_handicap"].to_list() == [None] * 6
    assert long_player_df["handicap"].to_list() == [0, 0, None, None, None, 1]

test_gotha_only_nonzero_handicap_marked = """
; CL[B]
; HA[h0]
 1 Someone Else      5k XX Clu  0 0 0 0  4+/b  3-/w   2+/w1 4-/w2 |11111111
 2 This The_Guy      9k XX Clu  0 0 0 0  3-/w1 4+/w1  1-/b1 3+/b  |22222222
 3 Issa Me           10 XX Ano  0 0 0 0  2+/b1 1+/b   4-/b1 2-/w  |33333333
 4 Stilla Me         11 IT Bari 0 0 0 0  1-/b  2-/b1  3+/w1 1+/b2 |44444444"""

def test_nonzero_handicap_marked():
    t_id = "T000103F"
    info_df, games_df = parsing.tournament_as_df(test_gotha_only_nonzero_handicap_marked, t_id)
    player_dfs = games_df.partition_by(["pin"], as_dict=True)
    first_player_df = player_dfs[(11111111,)].sort("round_number")
    assert first_player_df["handicap"].to_list() == [0, 0, 1, 2]
    second_player_df = player_dfs[(22222222,)].sort("round_number")
    assert second_player_df["handicap"].to_list() == [1, 1, 1, 0]
    third_player_df = player_dfs[(33333333,)].sort("round_number")
    assert third_player_df["handicap"].to_list() == [1, 0, 1, 0]
    fourth_player_df = player_dfs[(44444444,)].sort("round_number")
    assert fourth_player_df["handicap"].to_list() == [0, 1, 1, 2]

test_gotha_higher_than_9_explicit_handicap = """
; CL[C]
; HA[h0]
;
; Pl Name                            Rk Co Club  CAT  NBW  SOSSODOS
 1 Smith John            2k XX  ClubA  1    4    7    7    9+/b2   5+/w2   10+/w2  2+/w9    |11111111
 2 Johnson Michael      11k YY  ClubB  1    3    11   7    7+/b3   3+/w5   8+/w2   1-/b9    |22222222
 3 Brown Emily          16k ZZ  ClubC  1    3    7    4    12+/w3  2-/b5   7+/b8   8+/b3    |33333333
 4 Williams David        5k XX  ClubA  1    3    6    4    5-/b1   10+/b1  9+/b5   6+/b1    |44444444
 5 Jones Christopher     4k YY  ClubB  1    2    10   4    4+/w1   1-/b2   6-/w0   9+/b4    |55555555
 6 Garcia Daniel         4k ZZ  ClubC  1    2    7    3    10+/w0  9-/b4   5+/b0   4-/w1    |66666666
 7 Martinez Jessica      8k XX  ClubA  1    2    7    1    2-/w3   11+/w4  3-/w8   12+/w11  |77777777
 8 Rodriguez Jennifer   13k YY  ClubB  1    2    7    1    11+/b1  12+/w7  2-/b2   3-/w3    |88888888
 9 Wilson James          1d ZZ  ClubC  1    1    11   2    1-/w2   6+/w4   4-/w5   5-/w4    |99999999
10 Martinez Linda        4k XX  ClubA  1    1    10   1    6-/b0   4-/w1   1-/b2   11+/w8   |10101010
11 Hernandez Joshua     12k YY  ClubB  1    1    5    0    8-/w1   7-/b4   12+/w7  10-/b8   |11112222
12 Lopez Sarah          19k ZZ  ClubC  1    0    8    0    3-/b3   8-/b7   11-/b7  7-/b11   |12121212
"""

def test_higher_than_9_handicap_parsing():
    t_id = "T000104F"
    info_df, games_df = parsing.tournament_as_df(test_gotha_higher_than_9_explicit_handicap, t_id)
    player_dfs = games_df.partition_by(["pin"], as_dict=True)
    seventh_player_df = player_dfs[(77777777,)].sort("round_number")
    assert seventh_player_df["handicap"].to_list() == [3, 4, 8, 11]
    eighth_player_df = player_dfs[(88888888,)].sort("round_number")
    assert eighth_player_df["handicap"].to_list() == [1, 7, 2, 3]
    ninth_player_df = player_dfs[(99999999,)].sort("round_number")
    assert ninth_player_df["handicap"].to_list() == [2, 4, 5, 4]