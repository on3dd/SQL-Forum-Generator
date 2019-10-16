CREATE TABLE "categories"
(
    "id"        uuid         NOT NULL,
    "name"      varchar(255) NOT NULL,
    "parent_id" uuid         NOT NULL,
    CONSTRAINT "categories_pk" PRIMARY KEY ("id")
) WITH (
      OIDS= FALSE
    );

-- ALTER TABLE "categories"
--     ADD CONSTRAINT "categories_fk0" FOREIGN KEY ("parent_id") REFERENCES "categories" ("id");

