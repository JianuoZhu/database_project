create table t_user(
    mid NUMERIC primary key,
    name varchar(100) not null ,
    sex varchar(100),
    birthday varchar(100),
    level int not null,
    sign varchar(100),
    identity varchar(100),
    check (sex in('男', '女', '保密')),
    check (identity in('user','superuser'))
);
create table follows(
    followee numeric,
    follower numeric,
    foreign key (followee) references t_user(mid),
    foreign key (follower) references t_user(mid)
);