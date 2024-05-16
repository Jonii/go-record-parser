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
    assert info_df["CM"][0].strip() == "Test comment"
    assert info_df["tournament"][0] == t_id

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