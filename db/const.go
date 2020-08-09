package db

// SchemaQuery represents a query for db schema initialization
const SchemaQuery = `CREATE SCHEMA IF NOT EXISTS "hw_db"`

// TablesQuery represents a query for db tables initialization
const TablesQuery = `
	DROP TABLE IF EXISTS  public.categories CASCADE;
	DROP TABLE IF EXISTS  public.users CASCADE;
	DROP TABLE IF EXISTS  public.messages CASCADE;
	CREATE UNLOGGED TABLE IF NOT EXISTS public.messages (
	"id" uuid NOT NULL,
	"text" text NOT NULL,
	"category_id" uuid NOT NULL,
	"posted_at" timestamptz NOT NULL,
	"author_id" uuid NOT NULL
	) WITH (
	OIDS=FALSE
	);

	CREATE UNLOGGED TABLE IF NOT EXISTS  public.categories (
	"id" uuid NOT NULL,
	"name" varchar(255) NOT NULL,
	"parent_id" uuid
	) WITH (
	OIDS=FALSE
	);

	CREATE UNLOGGED TABLE IF NOT EXISTS  public.users (
	"id" uuid NOT NULL,
	"name" varchar(255) NOT NULL
	) WITH (
	OIDS=FALSE
	);

	ALTER TABLE public.users SET (autovacuum_enabled = false);
	ALTER TABLE public.categories SET (autovacuum_enabled = false);
	ALTER TABLE public.messages SET (autovacuum_enabled = false);
`
