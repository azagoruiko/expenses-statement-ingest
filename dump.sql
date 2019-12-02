--
-- PostgreSQL database dump
--

-- Dumped from database version 12.1 (Debian 12.1-1.pgdg100+1)
-- Dumped by pg_dump version 12.1 (Ubuntu 12.1-1.pgdg18.04+1)

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

--
-- Name: postgres; Type: DATABASE; Schema: -; Owner: postgres
--

CREATE DATABASE postgres WITH TEMPLATE = template0 ENCODING = 'UTF8' LC_COLLATE = 'en_US.utf8' LC_CTYPE = 'en_US.utf8';


ALTER DATABASE postgres OWNER TO postgres;

\connect postgres

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

--
-- Name: DATABASE postgres; Type: COMMENT; Schema: -; Owner: postgres
--

COMMENT ON DATABASE postgres IS 'default administrative connection database';


--
-- Name: expenses; Type: SCHEMA; Schema: -; Owner: postgres
--

CREATE SCHEMA expenses;


ALTER SCHEMA expenses OWNER TO postgres;

SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- Name: transactions; Type: TABLE; Schema: expenses; Owner: postgres
--

CREATE TABLE expenses.transactions (
    id character varying(256) NOT NULL,
    transaction_date timestamp without time zone NOT NULL,
    amount numeric(10,2) NOT NULL,
    description text NOT NULL,
    tags character varying(48)[]
);


ALTER TABLE expenses.transactions OWNER TO postgres;

--
-- Data for Name: transactions; Type: TABLE DATA; Schema: expenses; Owner: postgres
--

COPY expenses.transactions (id, transaction_date, amount, description, tags) FROM stdin;
\.


--
-- Name: transactions operations_pk; Type: CONSTRAINT; Schema: expenses; Owner: postgres
--

ALTER TABLE ONLY expenses.transactions
    ADD CONSTRAINT operations_pk PRIMARY KEY (id);


--
-- Name: transactions_tags_index; Type: INDEX; Schema: expenses; Owner: postgres
--

CREATE INDEX transactions_tags_index ON expenses.transactions USING gin (tags);


--
-- PostgreSQL database dump complete
--

