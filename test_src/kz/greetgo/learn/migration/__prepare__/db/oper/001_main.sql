create table client (
  id bigint not null primary key,

  surname varchar(300) not null,
  name varchar(300) not null,
  patronymic varchar(300),
  birth_date date not null,

  actual smallint not null default 0,
  cia_id varchar(100)
);;
create sequence s_client start with 1000000;;
