# Tools for parsing go tournament records

This project is collection of tools used to analyze go tournament data and specifically to try out alternate rating calculation methods.

Main features at the moment are `parsing.py`, which allows parsing gotha string into polars dataframe, and `gor_calculator.py`, which implements standard gor calculation, aiming for parity with EGD method.