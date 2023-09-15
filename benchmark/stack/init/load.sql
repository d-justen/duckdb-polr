CREATE TABLE account (
    id            integer unique not null,
    display_name  varchar,
    location      varchar,
    about_me      varchar,
    website_url   varchar
);


CREATE TABLE answer (
    id                  integer not null,
    site_id             integer,
    question_id         integer,
    creation_date       timestamp,
    deletion_date       timestamp,
    score               integer,
    view_count          integer,
    body                varchar,
    owner_user_id       integer,
    last_editor_id      integer,
    last_edit_date      timestamp,
    last_activity_date  timestamp,
    title               varchar
);

CREATE TABLE badge (
    site_id      integer,
    user_id      integer,
    name         varchar,
    date         timestamp
);

CREATE TABLE comment (
    id           integer not null,
    site_id      integer,
    post_id      integer,
    user_id      integer,
    score        integer,
    body         varchar,
    date         timestamp
);


CREATE TABLE post_link (
    site_id       integer,
    post_id_from  integer,
    post_id_to    integer,
    link_type     integer,
    date          timestamp
);

CREATE TABLE question (
    id                  integer not null,
    site_id             integer,
    accepted_answer_id  integer,
    creation_date       timestamp,
    deletion_date       timestamp,
    score               integer,
    view_count          integer,
    body                varchar,
    owner_user_id       integer,
    last_editor_id      integer,
    last_edit_date      timestamp,
    last_activity_date  timestamp,
    title               varchar,
    favorite_count      integer,
    closed_date         timestamp,
    tagstring           varchar
);

CREATE TABLE site (
    site_id      integer unique not null,
    site_name    varchar
);

CREATE TABLE so_user (
    id                integer not null,
    site_id           integer,
    reputation        integer,
    creation_date     timestamp,
    last_access_date  timestamp,
    upvotes           integer,
    downvotes         integer,
    account_id        integer
);

CREATE TABLE tag (
    id           integer not null,
    site_id      integer,
    name         varchar
);

CREATE TABLE tag_question (
    question_id  integer,
    tag_id       integer,
    site_id      integer
);

COPY account FROM '/Users/d-justen/Downloads/stack_bench/account.csv' (DELIMITER '|', HEADER);
COPY answer FROM '/Users/d-justen/Downloads/stack_bench/answer.csv' (DELIMITER '|', HEADER);
COPY badge FROM '/Users/d-justen/Downloads/stack_bench/badge.csv' (DELIMITER '|', HEADER);
COPY comment FROM '/Users/d-justen/Downloads/stack_bench/comment.csv' (DELIMITER '|', HEADER);
COPY post_link FROM '/Users/d-justen/Downloads/stack_bench/post_link.csv' (DELIMITER '|', HEADER);
COPY question FROM '/Users/d-justen/Downloads/stack_bench/question.csv' (DELIMITER '|', HEADER);
COPY site FROM '/Users/d-justen/Downloads/stack_bench/site.csv' (DELIMITER '|', HEADER);
COPY so_user FROM '/Users/d-justen/Downloads/stack_bench/so_user.csv' (DELIMITER '|', HEADER);
COPY tag FROM '/Users/d-justen/Downloads/stack_bench/tag.csv' (DELIMITER '|', HEADER);
COPY tag_question FROM '/Users/d-justen/Downloads/stack_bench/tag_question.csv' (DELIMITER '|', HEADER);