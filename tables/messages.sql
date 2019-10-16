CREATE TABLE "messages"
(
    "id"          uuid NOT NULL,
    "text"        TEXT NOT NULL,
    "category_id" uuid NOT NULL,
    "posted_at"   TIME NOT NULL,
    "author_id"   uuid NOT NULL,
    CONSTRAINT "messages_pk" PRIMARY KEY ("id")
) WITH (
      OIDS= FALSE
    );

-- ALTER TABLE "messages"
--     ADD CONSTRAINT "messages_fk0" FOREIGN KEY ("category_id") REFERENCES "categories" ("id");
-- ALTER TABLE "messages"
--     ADD CONSTRAINT "messages_fk1" FOREIGN KEY ("author_id") REFERENCES "users" ("id");

