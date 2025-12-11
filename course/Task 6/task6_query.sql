DROP FUNCTION IF EXISTS generate_fake_users(text, integer, integer, integer);
DROP TABLE IF EXISTS names CASCADE;
DROP TABLE IF EXISTS locales CASCADE;

CREATE TABLE locales (
    code TEXT PRIMARY KEY,
    description TEXT
);

CREATE TABLE names (
    id SERIAL PRIMARY KEY,
    locale TEXT REFERENCES locales(code),
    type   TEXT CHECK (type IN ('first','last')),
    value  TEXT NOT NULL
);

INSERT INTO locales(code, description) VALUES
('en_US','English (USA)'), ('de_DE','German (Germany)');

INSERT INTO names(locale,type,value) VALUES
('en_US','first','John'),('en_US','first','Emily'),
('en_US','last','Smith'),('en_US','last','Johnson'),
('de_DE','first','Hans'),('de_DE','first','Anna'),
('de_DE','last','Müller'),('de_DE','last','Schneider');

CREATE OR REPLACE FUNCTION generate_fake_users(
    p_locale       TEXT,
    p_seed         INT,
    p_batch_index  INT,
    p_batch_size   INT DEFAULT 10
)
RETURNS TABLE(full_name TEXT, email TEXT, phone TEXT)
LANGUAGE plpgsql
AS $$
BEGIN
    
    PERFORM setseed( ((p_seed + p_batch_index) % 100)::float / 100.0 );

    RETURN QUERY
    SELECT
        (
          (SELECT n1.value FROM names n1
             WHERE n1.locale = p_locale AND n1.type = 'first'
             ORDER BY random() LIMIT 1)
          || ' ' ||
          (SELECT n2.value FROM names n2
             WHERE n2.locale = p_locale AND n2.type = 'last'
             ORDER BY random() LIMIT 1)
        ) AS full_name,
        lower(substr(md5(random()::text), 1, 8)) || '@example.com' AS email,
        CASE
          WHEN p_locale = 'de_DE'
            THEN '+49 ' || floor(random()*900000000 + 100000000)::TEXT
          ELSE '+1-' || floor(random()*900000000 + 100000000)::TEXT
        END AS phone
    FROM generate_series(1, p_batch_size);
END;
$$;


