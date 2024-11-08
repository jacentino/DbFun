namespace DbFun.Firebird.IntegrationTests

open DbFun.Core
open Commons
open Models

module TestQueries = 

    let getBlog = query.Sql<int, Blog>(
        "select id, name, title, description, owner, createdAt, modifiedAt, modifiedBy from blog where id = @id", "id")

    let insertBlog = query.Sql<Blog, unit>(
        "insert into blog (id, name, title, description, owner, createdAt, modifiedAt, modifiedBy) 
         values (@id, @name, @title, @description, @owner, @createdAt, @modifiedAt, @modifiedBy)")

    let batchInsertBlogs = batch.Command<Blog>(
        "insert into blog (id, name, title, description, owner, createdAt, modifiedAt, modifiedBy) 
         values (@id, @name, @title, @description, @owner, @createdAt, @modifiedAt, @modifiedBy)")
