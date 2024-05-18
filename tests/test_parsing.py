import src.parsing as parsing


test_gotha = """
; CL[A]
; EV[Test tournament]
; PC[FI, Helsinki]
; DT[2023-03-29,2023-03-31]
; HA[h9]
; KM[6.5]
; TM[90]
; CM[ Test comment ]
;.
; Test tournament
; 29/03/2023
;
; Pl Name                   Grd Co Club  MMS  SOS SOSO
 1 Voittaja Ykkonen         4d SE  Stock  36   206  1231 0    5+/b0   4-/w0   3+/b0   2+/w0   5+/b0   4+/b0    |14011111
 2 Jaba Kakkonen            3d NO  Oslo   36   205  1238 0    3+/b0   5+/b0   4-/b0   1-/w0   4+/b0   5-/b0    |10222222
 3 Tyyppi Kolmonen          4d FI  Heh    35   206  1228 0    2-/w0   0=      1-/w0   4-/b0   0=      0=       |10333333
 4 Peluri Nelja             3d ES  Pol    35   205  1224 0    0=      1+/b0   2+/w0   3+/w0   2-/w0   1-/w0    |15444444
 5 Pelaaja Viisi            2d FR  Mel    35   203  1224 0    1-/w0   2-/w0   0=      0=      1-/b0   2+/w0    |14555555
"""

def test_tournament_info():
    t_id = "T000101F"
    info_df, games_df = parsing.tournament_as_df(test_gotha, t_id)
    assert info_df["CL"][0] == "A"
    assert info_df["EV"][0] == "Test tournament"
    assert info_df["PC"][0] == "FI, Helsinki"
    assert info_df["DT"][0] == "2023-03-29,2023-03-31"
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
        assert rounds_dfs["rank"].to_list() == ["4d", "3d", "4d", "3d", "2d"]
        assert rounds_dfs["pin"].to_list() == [14011111, 10222222, 10333333, 15444444, 14555555]

def test_parsing_games():
    t_id = "T000101F"
    info_df, games_df = parsing.tournament_as_df(test_gotha, t_id)
    player_dfs = games_df.partition_by(["pin"], as_dict=True)
    first_player_df = player_dfs[(14011111,)].sort("round_number")
    assert first_player_df["result"].to_list() == ["+", "-", "+", "+", "+", "+"]
    assert first_player_df["color"].to_list() == ["b", "w", "b", "w", "b", "b"]
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
