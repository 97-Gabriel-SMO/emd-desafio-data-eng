CREATE TABLE IF NOT EXISTS brt_raw (
    codigo SERIAL PRIMARY KEY,
    placa VARCHAR,
    linha VARCHAR,
    latitude FLOAT,
    longitude FLOAT,
    dataHora TIMESTAMP,
    velocidade FLOAT,
    id_migracao_trajeto VARCHAR,
    sentido VARCHAR,
    trajeto VARCHAR,
    hodometro FLOAT,
    direcao BOOLEAN
);