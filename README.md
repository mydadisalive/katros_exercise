# katros_exercise
katros exercise

files:
CalcID.py - simulates a calculation
parallel.py - a framework to run multiple calculations in parallel
ids_file - an example input file

parallel.py runs CalcID by using subprocess method and not by importing it (as if it was any executable).

To disable busy time policy (bonus question) in parallel.py change ENABLE_BUSY_TIME_POLICY to False
