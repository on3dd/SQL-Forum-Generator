CREATE TABLE "users"
(
    "id"   uuid         NOT NULL,
    "name" varchar(255) NOT NULL,
    CONSTRAINT "users_pk" PRIMARY KEY ("id")
) WITH (
      OIDS= FALSE
    );

