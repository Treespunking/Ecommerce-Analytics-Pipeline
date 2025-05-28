-- ./scripts/init.sql
DO
$do$
BEGIN
   IF NOT EXISTS (
      SELECT FROM pg_catalog.pg_user
      WHERE usename = 'postgres') THEN

      CREATE USER postgres WITH PASSWORD '2409';
   END IF;
END
$do$;

-- Check if the database exists before creating it
SELECT 'CREATE DATABASE ecommerce_analytics OWNER postgres;'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'ecommerce_analytics')\gexec