-- Accounts. Emails are stored lower-case by the application, so the plain UNIQUE constraint is case-insensitive
-- in effect, and also catches two concurrent registrations of the same address.
CREATE TABLE users (
  id            UUID PRIMARY KEY,
  email         VARCHAR(254) NOT NULL UNIQUE,
  password_hash VARCHAR(100) NOT NULL,
  role          VARCHAR(20)  NOT NULL CHECK (role IN ('USER','OPERATOR','ADMIN')),
  created_at    TIMESTAMPTZ NOT NULL,
  updated_at    TIMESTAMPTZ NOT NULL
);
