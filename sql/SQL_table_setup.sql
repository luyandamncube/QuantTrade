CREATE TABLE symbols (
    id SERIAL PRIMARY KEY,
    symbol VARCHAR(20) UNIQUE NOT NULL,
    name VARCHAR(255),
    type VARCHAR(20),               -- 'equity', 'etf', 'crypto', 'index'
    exchange VARCHAR(50),
    sector VARCHAR(100),
    industry VARCHAR(100),
    first_trade_date DATE,
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE price_daily (
    id SERIAL PRIMARY KEY,
    symbol_id INT REFERENCES symbols(id),
    date DATE NOT NULL,
    open NUMERIC,
    high NUMERIC,
    low NUMERIC,
    close NUMERIC,
    adj_close NUMERIC,
    volume BIGINT,
    UNIQUE(symbol_id, date)
);

CREATE TABLE price_minute (
    id BIGSERIAL PRIMARY KEY,
    symbol_id INT REFERENCES symbols(id),
    timestamp TIMESTAMPTZ NOT NULL,
    open NUMERIC,
    high NUMERIC,
    low NUMERIC,
    close NUMERIC,
    volume BIGINT,
    UNIQUE(symbol_id, timestamp)
);

CREATE TABLE price_tick (
    id BIGSERIAL PRIMARY KEY,
    symbol_id INT REFERENCES symbols(id),
    timestamp TIMESTAMPTZ NOT NULL,
    bid NUMERIC,
    ask NUMERIC,
    last_price NUMERIC,
    bid_size INT,
    ask_size INT,
    last_size INT,
    UNIQUE(symbol_id, timestamp)
);

-- /////////////////////////////////////// OPTIONS ///////////////////////////////////////

CREATE TABLE options_contracts (
    id SERIAL PRIMARY KEY,
    underlying_id INT REFERENCES symbols(id),
    option_symbol VARCHAR(50) UNIQUE NOT NULL,  -- e.g., AAPL230120C150
    expiration DATE NOT NULL,
    strike NUMERIC NOT NULL,
    type CHAR(1) CHECK (type IN ('C', 'P')),    -- Call or Put
    listed_date DATE,
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE options_quotes_daily (
    id SERIAL PRIMARY KEY,
    contract_id INT REFERENCES options_contracts(id),
    date DATE NOT NULL,
    bid NUMERIC,
    ask NUMERIC,
    last_price NUMERIC,
    volume BIGINT,
    open_interest BIGINT,
    implied_volatility NUMERIC,
    delta NUMERIC,
    gamma NUMERIC,
    theta NUMERIC,
    vega NUMERIC,
    UNIQUE(contract_id, date)
);

CREATE TABLE options_quotes_minute (
    id BIGSERIAL PRIMARY KEY,
    contract_id INT REFERENCES options_contracts(id),
    timestamp TIMESTAMPTZ NOT NULL,
    bid NUMERIC,
    ask NUMERIC,
    last_price NUMERIC,
    volume BIGINT,
    open_interest BIGINT,
    implied_volatility NUMERIC,
    delta NUMERIC,
    gamma NUMERIC,
    theta NUMERIC,
    vega NUMERIC,
    UNIQUE(contract_id, timestamp)
);

CREATE TABLE options_quotes_tick (
    id BIGSERIAL PRIMARY KEY,
    contract_id INT REFERENCES options_contracts(id),
    timestamp TIMESTAMPTZ NOT NULL,
    bid NUMERIC,
    ask NUMERIC,
    last_price NUMERIC,
    bid_size INT,
    ask_size INT,
    last_size INT,
    implied_volatility NUMERIC,
    delta NUMERIC,
    gamma NUMERIC,
    theta NUMERIC,
    vega NUMERIC,
    UNIQUE(contract_id, timestamp)
);

-- /////////////////////////////////////// BACKTESTING ///////////////////////////////////////
CREATE TABLE backtest_results (
    id SERIAL PRIMARY KEY,
    strategy_name VARCHAR(100),
    start_date DATE,
    end_date DATE,
    cagr NUMERIC,
    max_drawdown NUMERIC,
    sharpe NUMERIC,
    sortino NUMERIC,
    total_trades INT,
    notes TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
