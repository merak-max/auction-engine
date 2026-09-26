CREATE TABLE IF NOT EXISTS auction_budget_accounts (
    campaign_id TEXT PRIMARY KEY,
    daily_budget_micros BIGINT NOT NULL CHECK (daily_budget_micros BETWEEN 1 AND 1000000000000),
    pacing_burst_micros BIGINT NOT NULL CHECK (pacing_burst_micros BETWEEN 0 AND daily_budget_micros),
    spent_micros BIGINT NOT NULL DEFAULT 0 CHECK (spent_micros BETWEEN 0 AND daily_budget_micros),
    utc_day DATE NOT NULL DEFAULT ((CURRENT_TIMESTAMP AT TIME ZONE 'UTC')::date)
);
CREATE TABLE IF NOT EXISTS auction_replays (
    utc_day DATE NOT NULL, auction_id TEXT NOT NULL,
    request_json JSONB NOT NULL, result_json JSONB NOT NULL,
    PRIMARY KEY (utc_day, auction_id)
);

-- One client round trip, one synchronous transaction. No async WAL settings.
-- VOLATILE uses fresh snapshots for statements after waiting on locks.
CREATE OR REPLACE FUNCTION auction_run_v1(req JSONB) RETURNS JSONB
LANGUAGE plpgsql VOLATILE AS $$
DECLARE
    day DATE := (statement_timestamp() AT TIME ZONE 'UTC')::date;
    seconds BIGINT := floor(extract(epoch FROM (statement_timestamp() AT TIME ZONE 'UTC')::time));
    request_id TEXT := req->>'auction_id';
    floor_price BIGINT := (req->>'floor_micros')::bigint;
    slots INT := greatest(1, coalesce((req->>'slots')::int, 1));
    ids TEXT[];
    locked_count INT;
    previous RECORD;
    eligible JSONB;
    winners JSONB := '[]';
    winner JSONB;
    result JSONB;
    price BIGINT;
    i INT;
    lock_key INT;
    removed BOOLEAN;
BEGIN
    req := jsonb_set(req, '{slots}', to_jsonb(slots));
    PERFORM pg_advisory_xact_lock(7101, hashtext(day::text || ':' || request_id));
    SELECT r.request_json, r.result_json INTO previous
      FROM auction_replays r WHERE r.utc_day=day AND r.auction_id=request_id;
    IF FOUND THEN
        IF jsonb_set(previous.request_json, '{slots}', to_jsonb(greatest(1, coalesce((previous.request_json->>'slots')::int, 1)))) <> req THEN
            RAISE EXCEPTION 'auction_id already used for a different request' USING ERRCODE='AU001';
        END IF;
        RETURN previous.result_json;
    END IF;

    SELECT array_agg(c.campaign_id) INTO ids FROM jsonb_to_recordset(req->'candidates') AS c(campaign_id TEXT);
    -- Queue on a stable logical account lock before touching mutable tuples.
    -- Repeated UPDATEs otherwise cause SELECT FOR UPDATE waiters to requeue
    -- on new tuple versions, giving some requests very long tails.
    -- Sort distinct lock keys, not IDs: hash collisions remain deadlock-safe.
    FOR lock_key IN SELECT DISTINCT hashtext(id) FROM unnest(ids) AS id ORDER BY 1 LOOP
        PERFORM pg_advisory_xact_lock(7102, lock_key);
    END LOOP;
    PERFORM a.campaign_id FROM auction_budget_accounts a WHERE a.campaign_id=ANY(ids)
      ORDER BY a.campaign_id COLLATE "C" FOR UPDATE;
    GET DIAGNOSTICS locked_count = ROW_COUNT;
    IF locked_count <> cardinality(ids) THEN
        RAISE EXCEPTION 'unknown campaign_id' USING ERRCODE='AU001';
    END IF;
    IF EXISTS(SELECT FROM auction_budget_accounts a WHERE a.campaign_id=ANY(ids) AND a.utc_day>day) THEN
        RAISE EXCEPTION 'UTC clock moved to a previous day' USING ERRCODE='AU002';
    END IF;
    UPDATE auction_budget_accounts a SET utc_day=day, spent_micros=0
      WHERE a.campaign_id=ANY(ids) AND a.utc_day<day;

    SELECT coalesce(jsonb_agg(jsonb_build_object('campaign_id', c.campaign_id,
        'bid_micros', c.bid_micros, 'headroom',
        least(a.daily_budget_micros, a.pacing_burst_micros + a.daily_budget_micros*seconds/86400)-a.spent_micros)
        ORDER BY c.bid_micros DESC, c.campaign_id COLLATE "C"), '[]') INTO eligible
      FROM jsonb_to_recordset(req->'candidates') AS c(campaign_id TEXT, bid_micros BIGINT)
      JOIN auction_budget_accounts a USING (campaign_id)
      WHERE c.bid_micros>=floor_price AND
        least(a.daily_budget_micros, a.pacing_burst_micros+a.daily_budget_micros*seconds/86400)-a.spent_micros>=floor_price;

    WHILE jsonb_array_length(eligible)>=slots LOOP
        winners := '[]';
        removed := false;
        FOR i IN 0..slots-1 LOOP
            winner := eligible->i;
            price := greatest(floor_price, coalesce((eligible->(i+1)->>'bid_micros')::bigint, floor_price));
            IF price > (winner->>'headroom')::bigint THEN
                eligible := eligible-i;
                removed := true;
                EXIT;
            END IF;
            winners := winners || jsonb_build_array(jsonb_build_object('winner_id', winner->>'campaign_id',
                'winning_bid_micros', (winner->>'bid_micros')::bigint, 'clearing_price_micros', price));
        END LOOP;
        EXIT WHEN NOT removed;
        winners := '[]';
    END LOOP;

    result := jsonb_build_object('auction_id', request_id, 'no_fill_reason', 'no_eligible_budget_or_bid');
    IF jsonb_array_length(winners)=slots THEN
        UPDATE auction_budget_accounts a SET spent_micros=a.spent_micros+w.clearing_price_micros
          FROM jsonb_to_recordset(winners) AS w(winner_id TEXT, clearing_price_micros BIGINT)
          WHERE a.campaign_id=w.winner_id;
        result := jsonb_build_object('auction_id', request_id) || (winners->0);
        IF slots>1 THEN result := result || jsonb_build_object('winners', winners); END IF;
    END IF;
    INSERT INTO auction_replays(utc_day, auction_id, request_json, result_json) VALUES(day, request_id, req, result);
    RETURN result;
END;
$$;

-- Pre-lock the union in canonical order so batches with overlapping auctions
-- cannot deadlock. Valid requests in a batch commit together; a client error
-- is isolated in its own subtransaction and cannot poison adjacent requests.
CREATE OR REPLACE FUNCTION auction_batch_v1(requests JSONB) RETURNS JSONB
LANGUAGE plpgsql VOLATILE AS $$
DECLARE
    req JSONB;
    results JSONB := '[]';
    lock_key INT;
    day TEXT := ((statement_timestamp() AT TIME ZONE 'UTC')::date)::text;
BEGIN
    FOR lock_key IN SELECT DISTINCT hashtext(day || ':' || (r->>'auction_id')) FROM jsonb_array_elements(requests) r ORDER BY 1 LOOP
        PERFORM pg_advisory_xact_lock(7101, lock_key);
    END LOOP;
    FOR lock_key IN SELECT DISTINCT hashtext(c->>'campaign_id') FROM jsonb_array_elements(requests) r,
      LATERAL jsonb_array_elements(r->'candidates') c ORDER BY 1 LOOP
        PERFORM pg_advisory_xact_lock(7102, lock_key);
    END LOOP;
    FOR req IN SELECT value FROM jsonb_array_elements(requests) LOOP
        BEGIN
            results := results || jsonb_build_array(jsonb_build_object('result',auction_run_v1(req)));
        EXCEPTION WHEN SQLSTATE 'AU001' OR SQLSTATE 'AU002' THEN
            results := results || jsonb_build_array(jsonb_build_object('error_code',SQLSTATE,'message',SQLERRM));
        END;
    END LOOP;
    RETURN results;
END;
$$;
