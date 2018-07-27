
CREATE TABLE ad (
    id serial primary key,
    "timestamp" timestamp with time zone,
    page_title char(250) NOT NULL,
    label char(250),
    email char(50),
    phone char(50),
    description text 
);
