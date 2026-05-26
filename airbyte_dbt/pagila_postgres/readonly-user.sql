CREATE USER your_read_only_user PASSWORD 'your_password';

GRANT CONNECT ON DATABASE postgres TO your_read_only_user;

GRANT USAGE ON SCHEMA public TO your_read_only_user;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO your_read_only_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO your_read_only_user;