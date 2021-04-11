# portfolio-manager

Portfolio Manager is an end-to-end portfolio booking system which supports multiple asset classes such as equities, fixed income and crypto-currencies. It aims to automate the following:

1. Dividends and Positions Tracking
2. Price, Volume and Dividend Market Data Extraction
3. P&L computation
4. Value at Risk (Phase 2)
5. Trade signals via automated slack notifications
6. Backtesting

<br>

## Configurations

- Set PYTHONPATH to the parent directory of the repository
- Create a virtual environment (venv) with dependencies from requirements.txt
- Add venv path to PATH
- Start a mySQL database
- Run scripts/seed_tables.py script in pf_manager > scripts to create and populate the initial tables

### Slack

- For slack alerts to work, create `slackcred.json` at the root of the repository
- Create an app in slack by following the steps [here](https://api.slack.com/start/building/bolt-python)
- Put the Bot User OAuth Token into a key value pair in the JSON file,e.g. `"BOT_TOKEN": "xoxb-..."`
- Create a new channel in slack
- Invite the bot ("app") to the channel by typing /invite -> Add apps to the channel

<br>

## Market Data

There are multiple market data engines in this repository:

| Engine   | Description                         |
| :------- | :---------------------------------- |
| eod      | Req/Resp via Yahoo & Google finance |
| intraday | Websockets via Tradingview          |
| dividend | Async Req/Resp via Dividends.sg     |

<br>

### Dividends

Dividends are aggregated on ticker and ex-date, by the sum of the total payouts and stored into `market_dividends` table. The market data extraction is periodically extracted in an automated manner via the relevant market data engines, which reads the required tickers from the `reference_data` table.

The portfolio manager then takes the market dividends and compute the actual amount received by the respective portfolios before storing it in the `dividends` table.
