DROP TABLE IF EXISTS fligth_data;

CREATE EXTERNAL TABLE fligth_data (
    id INT,
    Year INT,
    Month INT,
    DayofMonth INT,
    DayOfWeek int,
    DepTime int,
    CRSDepTime int,
    ArrTime int,
    CRSArrTime int,
    UniqueCarrier String,
    FlightNum int,
    TailNum String,
    ActualElapsedTime int,
    CRSElapsedTime int,
    AirTime int,
    ArrDelay int,
    DepDelay int,
    Origin String,
    Dest String,
    Distance int,
    TaxiIn int,
    TaxiOut int,
    Cancelled int,
    CancellationCode String,
    Diverted int,
    CarrierDelay int,
    WeatherDelay int,
    NASDelay int,
    SecurityDelay int,
    LateAircraftDelay int
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION "${INPUT}"
TBLPROPERTIES ("skip.header.line.count"="1");

INSERT OVERWRITE DIRECTORY "${OUTPUT}"
SELECT Year, avg((NASDelay /ArrDelay)*100)
    FROM fligth_data 
    GROUP BY Year
    ORDER BY Year ASC;
