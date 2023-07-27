CREATE TABLE IF NOT EXISTS brt_raw (
    id  SERIAL PRIMARY KEY,
    codigo VARCHAR,
    placa VARCHAR,
    linha VARCHAR,
    latitude FLOAT,
    longitude FLOAT,
    dataHora BIGINT,
    velocidade FLOAT,
    id_migracao_trajeto VARCHAR,
    sentido VARCHAR,
    trajeto VARCHAR,
    hodometro FLOAT,
    direcao VARCHAR
);