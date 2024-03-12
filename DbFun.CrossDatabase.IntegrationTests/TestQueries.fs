namespace DbFun.CrossDatabase.IntegrationTests

open DbFun.Core
open Commons
open Models

module MsSqlQueries = 
    
    open DbFun.MsSql.Builders
    
    let query = QueryBuilder<Discriminator>(MsSqlServer, createConnection)
       
    let insertBlog = query.Sql<Blog, unit>(
        "insert into blog (name, title, description, owner, createdAt, modifiedAt, modifiedBy) 
         values (@name, @title, @description, @owner, @createdAt, @modifiedAt, @modifiedBy)")

    let deleteAllButFirstBlog = 
        query.Sql<unit, unit> "delete from blog where id > 1"

    let getNumberOfBlogs = query.Sql<unit, int> "select count(*) from blog"


module MySqlQueries = 

    open DbFun.Core.Builders

    let query = QueryBuilder<Discriminator>(MySql, createConnection)
       
    let insertBlog = query.Sql<Blog, unit>(
        "insert into blog (id, name, title, description, owner, createdAt, modifiedAt, modifiedBy) 
         values (@id, @name, @title, @description, @owner, @createdAt, @modifiedAt, @modifiedBy)")

    let deleteAllButFirstBlog = 
        query.Sql<unit, unit> "delete from blog where id > 1"

    let getNumberOfBlogs = query.Sql<unit, int> "select count(*) from blog"


module PostgresQueries = 

    open DbFun.Core.Builders

    let query = QueryBuilder<Discriminator>(Postgres, createConnection)
       
    let insertBlog = query.Sql<Blog, unit>(
        "insert into blog (blogid, name, title, description, owner, createdAt, modifiedAt, modifiedBy) 
         values (@id, @name, @title, @description, @owner, @createdAt, @modifiedAt, @modifiedBy)")

    let deleteAllButFirstBlog = 
        query.Sql<unit, unit> "delete from blog where blogid > 1"

    let getNumberOfBlogs = query.Sql<unit, int> "select count(*) from blog"
