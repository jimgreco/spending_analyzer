-- REVIEW PROPOSAL ONLY. Not applied. Requires per-app catalog/ACL, restore,
-- secure credential handoff and coordinator cutover approval described in database-runtime-access.md.
-- Adoption must have completed before this script. This script creates NOLOGIN roles;
-- enabling approved migration/runtime logins and private credential injection is an owner-only step.
-- This removes PUBLIC CONNECT/TEMP only on the selected app database after consumer review.
-- Rights inherited in other databases remain a separate reviewed gate.
\set ON_ERROR_STOP on
BEGIN;
DO $$ BEGIN IF current_database() <> 'spending' THEN RAISE EXCEPTION 'Wrong database'; END IF; END $$;
DO $$ BEGIN IF to_regclass('public.app_schema_versions') IS NULL THEN RAISE EXCEPTION 'Adopt schema first'; END IF; END $$;
CREATE ROLE spending_owner NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS;
CREATE ROLE spending_migrator NOLOGIN NOINHERIT NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS;
CREATE ROLE spending_runtime NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS;
GRANT spending_owner TO spending_migrator WITH INHERIT FALSE;
-- Add explicit CONNECT grants for any additional catalog-verified legitimate consumers before approval.
REVOKE ALL ON DATABASE spending FROM PUBLIC;
GRANT CONNECT ON DATABASE spending TO spending_migrator, spending_runtime;
ALTER SCHEMA public OWNER TO spending_owner;
-- Before approval, enumerate and preserve any additional legitimate schema users.
REVOKE CREATE ON SCHEMA public FROM PUBLIC;
GRANT USAGE ON SCHEMA public TO spending_runtime;
ALTER TABLE public.cards OWNER TO spending_owner;
REVOKE ALL ON public.cards FROM PUBLIC;
GRANT SELECT, INSERT, UPDATE, DELETE ON public.cards TO spending_runtime;
ALTER TABLE public.categories OWNER TO spending_owner;
REVOKE ALL ON public.categories FROM PUBLIC;
GRANT SELECT, INSERT, UPDATE, DELETE ON public.categories TO spending_runtime;
ALTER TABLE public.categorization_settings OWNER TO spending_owner;
REVOKE ALL ON public.categorization_settings FROM PUBLIC;
GRANT SELECT, INSERT, UPDATE, DELETE ON public.categorization_settings TO spending_runtime;
ALTER TABLE public.invited_users OWNER TO spending_owner;
REVOKE ALL ON public.invited_users FROM PUBLIC;
GRANT SELECT, INSERT, UPDATE, DELETE ON public.invited_users TO spending_runtime;
ALTER TABLE public.tags OWNER TO spending_owner;
REVOKE ALL ON public.tags FROM PUBLIC;
GRANT SELECT, INSERT, UPDATE, DELETE ON public.tags TO spending_runtime;
ALTER TABLE public.transaction_amount_changes OWNER TO spending_owner;
REVOKE ALL ON public.transaction_amount_changes FROM PUBLIC;
GRANT SELECT, INSERT, UPDATE, DELETE ON public.transaction_amount_changes TO spending_runtime;
ALTER TABLE public.transaction_tags OWNER TO spending_owner;
REVOKE ALL ON public.transaction_tags FROM PUBLIC;
GRANT SELECT, INSERT, UPDATE, DELETE ON public.transaction_tags TO spending_runtime;
ALTER TABLE public.transactions OWNER TO spending_owner;
REVOKE ALL ON public.transactions FROM PUBLIC;
GRANT SELECT, INSERT, UPDATE, DELETE ON public.transactions TO spending_runtime;
ALTER TABLE public.upload_jobs OWNER TO spending_owner;
REVOKE ALL ON public.upload_jobs FROM PUBLIC;
GRANT SELECT, INSERT, UPDATE, DELETE ON public.upload_jobs TO spending_runtime;
ALTER TABLE public.uploaded_files OWNER TO spending_owner;
REVOKE ALL ON public.uploaded_files FROM PUBLIC;
GRANT SELECT, INSERT, UPDATE, DELETE ON public.uploaded_files TO spending_runtime;
ALTER TABLE public.users OWNER TO spending_owner;
REVOKE ALL ON public.users FROM PUBLIC;
GRANT SELECT, INSERT, UPDATE, DELETE ON public.users TO spending_runtime;
ALTER TABLE public.app_schema_versions OWNER TO spending_owner;
REVOKE ALL ON public.app_schema_versions FROM PUBLIC;
GRANT SELECT ON public.app_schema_versions TO spending_runtime;
ALTER SEQUENCE public.cards_id_seq OWNER TO spending_owner;
REVOKE ALL ON SEQUENCE public.cards_id_seq FROM PUBLIC;
GRANT USAGE ON SEQUENCE public.cards_id_seq TO spending_runtime;
ALTER SEQUENCE public.categories_id_seq OWNER TO spending_owner;
REVOKE ALL ON SEQUENCE public.categories_id_seq FROM PUBLIC;
GRANT USAGE ON SEQUENCE public.categories_id_seq TO spending_runtime;
ALTER SEQUENCE public.invited_users_id_seq OWNER TO spending_owner;
REVOKE ALL ON SEQUENCE public.invited_users_id_seq FROM PUBLIC;
GRANT USAGE ON SEQUENCE public.invited_users_id_seq TO spending_runtime;
ALTER SEQUENCE public.tags_id_seq OWNER TO spending_owner;
REVOKE ALL ON SEQUENCE public.tags_id_seq FROM PUBLIC;
GRANT USAGE ON SEQUENCE public.tags_id_seq TO spending_runtime;
ALTER SEQUENCE public.transaction_amount_changes_id_seq OWNER TO spending_owner;
REVOKE ALL ON SEQUENCE public.transaction_amount_changes_id_seq FROM PUBLIC;
GRANT USAGE ON SEQUENCE public.transaction_amount_changes_id_seq TO spending_runtime;
ALTER SEQUENCE public.transactions_id_seq OWNER TO spending_owner;
REVOKE ALL ON SEQUENCE public.transactions_id_seq FROM PUBLIC;
GRANT USAGE ON SEQUENCE public.transactions_id_seq TO spending_runtime;
ALTER SEQUENCE public.uploaded_files_id_seq OWNER TO spending_owner;
REVOKE ALL ON SEQUENCE public.uploaded_files_id_seq FROM PUBLIC;
GRANT USAGE ON SEQUENCE public.uploaded_files_id_seq TO spending_runtime;
ALTER SEQUENCE public.users_id_seq OWNER TO spending_owner;
REVOKE ALL ON SEQUENCE public.users_id_seq FROM PUBLIC;
GRANT USAGE ON SEQUENCE public.users_id_seq TO spending_runtime;
ALTER FUNCTION public.set_transaction_original_amount() OWNER TO spending_owner;
REVOKE ALL ON FUNCTION public.set_transaction_original_amount() FROM PUBLIC;
ALTER DEFAULT PRIVILEGES FOR ROLE spending_owner REVOKE EXECUTE ON FUNCTIONS FROM PUBLIC;
-- New tables/sequences get no default runtime grants; each migration grants its reviewed objects.
-- Database/extension ownership and shared admin credentials are intentionally untouched.
COMMIT;
