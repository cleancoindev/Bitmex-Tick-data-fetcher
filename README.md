# Bitmex-Tick-data-fetcher

Fetch Bitmex historical Tick Data for analysis, ML, backtesting, ...

Insert into a Postgresql database or CSV files

## Requirements

```bash
npm install
cp sample.env .env
vim .env
```

## Usage

```bash
node tick>>postgres.js --start 2019-01-01 --end 2020-01-01
```

The data will be inserted into a `tickdata` table

or

```bash
node tick>>csv.js --start 2019-01-01 --end 2020-01-01
```

The files will be present in the `data/` folder
