CREATE TABLE census (
    "id" serial PRIMARY KEY,
    "year" varchar (10) NOT NULL,
    "zipcode" varchar (10) NOT NULL,
    "min_age" varchar (10),
    "max_age"  varchar(10),
    "gender" varchar(10)
);