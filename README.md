# product_analytics

city = nt - Нижный Тагил   min(lat) = 57.75689697, max(lat) = 58.009897 

city = a  - Архангельск    min(lat) = 64.35482801, max(lat) = 64.702556 

city = n  - Новошахтенск   min(lat) = 47.63534153, max(lat) = 47.903323

---Полезные запросы---
1. Создание таблицы из запроса

create table fediq_team.{name} as (
  select ...
);

2. Удаление таблицы

drop table fediq_team.{name}

3. Выдача прав на таблицу

grant all privileges on table fediq_team.{name} to 
idmashaantonenko, 
m8element,
yaroslawserow,
sad2017a
with grant option;

4. Добавление колонки

ALTER TABLE fediq_team.{name} ADD COLUMN {сol_name} integer DEFAULT 650000;

