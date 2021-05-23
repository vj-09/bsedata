import psycopg2

con = psycopg2.connect(database="marketdata", user="vadith", password="", host="127.0.0.1", port="5432")
print("Database opened successfully")

cur = con.cursor()
cur.execute(
    """
CREATE TABLE IF NOT EXISTS daily_data121 (
    currentValue float8,
    change float8,
    pChange float8,
    updatedOn date,
    priceBand varchar,
    securityID varchar,
    scripCode varchar,
    group_ varchar,
    faceValue varchar,
    industry varchar,
    previousClose float8,
    previousOpen float8,
    dayHigh float8,
    dayLow float8,
    week52High float8,
    week52Low float8,
    weightedAvgPrice float8,
    totalTradedValue float8,
    totalTradedQuantity float8,
    Week2AvgQuantity float8,
    marketCapFull float8,
    marketCapFreeFloat float8,
    upperPriceBand float8,
    lowerPriceBand float8
);

      """
      )
print("Table created successfully")
con.commit()



cur.execute("SELECT * from daily_data121")
rows = cur.fetchall()
print(rows)
con.close()