%default INPUT_FILE '/user/ec2-user/task1/input/2008.csv';
%default OUTPUT_FILE '/user/ec2-user/task1/output/TomsOptions.csv';
SET DEFAULT_PARALLEL 10;
raw_data =
  LOAD '$INPUT_FILE'
  USING org.apache.pig.piggybank.storage.CSVExcelStorage(',', 'NO_MULTILINE', 'UNIX', 'SKIP_INPUT_HEADER')
  AS ( Year:int, Month:int, DayofMonth:int, DayOfWeek:int, DepTime:float, CRSDepTime:int, ArrTime:float, CRSArrTime:int, UniqueCarrier, FlightNum:int, ActualElapsedTime:float,
  CRSElapsedTime:float, AirTime:float, ArrDelay:float, DepDelay:float, Origin, Dest, Distance:int, Cancelled:int);

first_trip_temp = FOREACH (
  FILTER raw_data BY DepTime <= 1200 AND Cancelled != 1 ) 
  GENERATE Origin, Dest, ArrDelay, Year, Month, DayofMonth, DayOfWeek;


second_trip_temp = FOREACH (
  FILTER raw_data BY DepTime >= 1200 AND Cancelled != 1 ) 
  GENERATE Origin, Dest, ArrDelay, Year, Month, DayofMonth, DayOfWeek;

first_trip = ORDER first_trip_temp BY Dest ASC;
second_trip = ORDER second_trip_temp BY Origin ASC;

trip_options_all = JOIN first_trip BY (Dest), second_trip BY (Origin);

trip_options = FILTER trip_options_all BY ABS(second_trip::DayOfWeek - first_trip::DayOfWeek) == 2;

trip_options = FOREACH trip_options GENERATE first_trip::Origin, first_trip::Dest, second_trip::Dest,( first_trip::ArrDelay + second_trip::ArrDelay) AS (Delay:float), first_trip::Year, first_trip::Month, first_trip::DayofMonth;

-- TODO:For each SOURCE->LAYOVER->DEST combination, select the trip with the shortest arrival delay here instead of in the query

STORE trip_options INTO '$OUTPUT_PATH';

