-- Migration file created on the 20th July 2022

CREATE TABLE IF NOT EXISTS users
(
    row_id     BIGSERIAL,
    user_id    uuid NOT NULL,
    email      TEXT NOT NULL,
    name       TEXT NOT NULL,
    surname    TEXT NOT NULL,
    created_at timestamptz DEFAULT now(),
    updated_at timestamptz DEFAULT now(),


    CONSTRAINT pk_user PRIMARY KEY (user_id),
    CONSTRAINT pk_email_user PRIMARY KEY (email)
);
