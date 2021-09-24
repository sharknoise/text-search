DROP TABLE IF EXISTS posts;
CREATE TABLE posts (
  id integer PRIMARY KEY,
  rubrics varchar[],
  text text NOT NULL,
  created_date timestamp
);