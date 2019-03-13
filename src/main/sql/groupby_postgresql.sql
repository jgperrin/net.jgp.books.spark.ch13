DROP TABLE public.ch13;

CREATE TABLE public.ch13(
  "firstName" character varying(255) COLLATE pg_catalog."default",
  "lastName" character varying(255) COLLATE pg_catalog."default",
  "state" character(2) COLLATE pg_catalog."default",
  quantity integer,
  revenue money,
  "timestamp" timestamp without time zone
)
WITH (
    OIDS = FALSE
)
TABLESPACE pg_default;

ALTER TABLE public.ch13
  OWNER to postgres;

INSERT INTO public.ch13("firstName", "lastName", "state", "quantity", "revenue", "timestamp") 
  VALUES 
    ('Jean Georges', 'Perrin', 'NC', 1, 300, to_timestamp(1551903533)),
    ('Jean Georges', 'Perrin', 'NC', 2, 120, to_timestamp(1551903567)),
    ('Jean Georges', 'Perrin', 'CA', 4, 75, to_timestamp(1551903599)),
    ('Holden', 'Karau', 'CA', 6, 37, to_timestamp(1551904299)),
    ('Ginni', 'Rometty', 'NY', 7, 91, to_timestamp(1551916792)),
    ('Holden', 'Karau', 'CA', 4, 153, to_timestamp(1552876129));

SELECT * FROM public.ch13;
												   
SELECT 
    "firstName", 
    "lastName", 
    "state", 
    SUM(quantity) AS sum_qty, 
    SUM(revenue) AS sum_rev, 
    AVG(revenue::numeric::float8) AS avg_rev 
  FROM public.ch13 
  GROUP BY ("firstName", "lastName", "state");
  