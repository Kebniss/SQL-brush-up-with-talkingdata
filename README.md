# SQL-brush-up-with-talkingdata

I first created the database using DataGrip tools. Below is reported the name and structure of each table.


CREATE TABLE public.gender_age (
  device_id BIGINT PRIMARY KEY NOT NULL,
  gender CHARACTER(1),
  age SMALLINT,
  label CHARACTER(10)
);


CREATE TABLE public.events (
  event_id BIGINT PRIMARY KEY NOT NULL,
  device_id BIGINT,
  datetime TIMESTAMP WITHOUT TIME ZONE,
  longitude DOUBLE PRECISION,
  latitude DOUBLE PRECISION,
  FOREIGN KEY (device_id) REFERENCES public.gender_age (device_id)
  MATCH SIMPLE ON UPDATE NO ACTION ON DELETE NO ACTION
);


CREATE TABLE public.phone_brand_device_model (
  device_id BIGINT PRIMARY KEY NOT NULL,
  phone_brand CHARACTER(100),
  device_model CHARACTER(100),
  FOREIGN KEY (device_id) REFERENCES public.gender_age (device_id)
  MATCH SIMPLE ON UPDATE NO ACTION ON DELETE NO ACTION
);


CREATE TABLE public.app_events (
  event_id BIGINT,
  app_id BIGINT PRIMARY KEY NOT NULL,
  is_installed BYTEA,
  is_active BYTEA,
  FOREIGN KEY (event_id) REFERENCES public.events (event_id)
  MATCH SIMPLE ON UPDATE NO ACTION ON DELETE NO ACTION
);


CREATE TABLE public.app_labels (
  app_id BIGINT,
  label_id BIGINT PRIMARY KEY NOT NULL,
  FOREIGN KEY (app_id) REFERENCES public.app_events (app_id)
  MATCH SIMPLE ON UPDATE NO ACTION ON DELETE NO ACTION
);