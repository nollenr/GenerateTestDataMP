insert into e values(1,'Domenic',	'Leaver',	5);
insert into e values(2,'Cleveland',	'Hewins',	1);
insert into e values(3,'Kakalina',	'Atherton',	8);
insert into e values(4,'Roxanna',	'Fairlie',	NULL);
insert into e values(5,'Hermie',	'Comsty',	4);
insert into e values(6,'Pooh',	'Goss',	8);
insert into e values(7,'Faulkner',	'Challiss',	5);
insert into e values(8,'Bobbe',	'Blakeway',	4);
insert into e values(9,'Laurene',	'Burchill',	1);
insert into e values(10,'Augusta',	'Gosdin',	8);



with recursive emp_hierarcy as (
    select id, first_name, last_name, boss_id, 0 as hierarchy_level
    from e
    where boss_id is null
    union all 
    select e.id, e.first_name, e.last_name, e.boss_id, hierarchy_level + 1
    from   e, emp_hierarcy h
    where  e.boss_id = h.id
)
select employee.id, employee.first_name, employee.last_name, employee.hierarchy_level as level, employee.boss_id, boss.first_name, boss.last_name
from   emp_hierarcy employee
       left join e boss on employee.boss_id = boss.id;


insert into c values ('Groningen',	'Heerenveen',	61.4);
insert into c values ('Groningen',	'Harlingen',	91.6);
insert into c values ('Harlingen',	'Wieringerwerf',	52.3);
insert into c values ('Wieringerwerf',	'Hoorn',	26.5);
insert into c values ('Hoorn',	'Amsterdam',	46.1);
insert into c values ('Amsterdam',	'Haarlem',	30);
insert into c values ('Heerenveen',	'Lelystad',	74);
insert into c values ('Lelystad',	'Amsterdam',	57.2);

with recursive possible_route as(
    SELECT  cr.city_to,
            cr.city_from || '->' ||cr.city_to AS route,
            cr.distance
      FROM  c cr
      WHERE cr.city_from = 'Groningen'
    UNION ALL
    SELECT cr.city_to, 
           pr.route || '->' || cr.city_to as route,
           cr.distance + pr.distance
    FROM   c cr, possible_route pr
    WHERE  cr.city_from = pr.city_to
)
select * from possible_route;
select route, distance from possible_route where city_to = 'Haarlem';

select city_from, city_to, total_distance
from   possible_route
where  city_to = 'Haarlem';



