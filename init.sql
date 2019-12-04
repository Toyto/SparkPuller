CREATE DATABASE healthdb;

CREATE TABLE "patient" (
  "id" serial NOT NULL PRIMARY KEY,
  "source_id" varchar(255) NOT NULL,
  "birth_date" date,
  "gender" varchar(255),
  "race_code" varchar(255),
  "race_code_system" varchar(255),
  "ethnicity_code" varchar(255),
  "ethnicity_code_system" varchar(255),
  "country" varchar(255)
);

CREATE TABLE "encounter" (
  "id" serial NOT NULL PRIMARY KEY,
  "source_id" varchar(255) NOT NULL,
  "patient_id" integer not null references patient(id) DEFERRABLE INITIALLY DEFERRED,
  "start_date" timestamp NOT NULL,
  "end_date" timestamp NOT NULL,
  "type_code" varchar(255),
  "type_code_system" varchar(255)
);

CREATE TABLE "procedure" (
  "id" serial NOT NULL PRIMARY KEY,
  "source_id" varchar(255) NOT NULL,
  "patient_id" integer not null references patient(id) DEFERRABLE INITIALLY DEFERRED,
  "encounter_id" integer references patient(id) DEFERRABLE INITIALLY DEFERRED,
  "procedure_date" date NOT NULL,
  "type_code" varchar(255) NOT NULL,
  "type_code_system" varchar(255) NOT NULL
);

CREATE TABLE "observation" (
  "id" serial NOT NULL PRIMARY KEY,
  "source_id" varchar(255) NOT NULL,
  "patient_id" integer not null references patient(id) DEFERRABLE INITIALLY DEFERRED,
  "encounter_id" integer references patient(id) DEFERRABLE INITIALLY DEFERRED,
  "observation_date" date NOT NULL,
  "type_code" varchar(255) NOT NULL,
  "type_code_system" varchar(255) NOT NULL,
  "value" decimal NOT NULL,
  "unit_code" varchar(255),
  "unit_code_system" varchar(255)
);