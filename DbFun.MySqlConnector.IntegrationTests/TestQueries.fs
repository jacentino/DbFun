namespace DbFun.MySqlConnector.IntegrationTests

open DbFun.Core
open Commons
open Models
open DbFun.MySqlConnector

module TestQueries = 

    let query = query.LogCompileTimeErrors()

    let getBlog = query.Sql<int, Blog>(
        "select id, name, title, description, owner, createdAt, modifiedAt, modifiedBy from blog where id = @id", "id")

    let spInsertBlog = query.Proc<Blog, unit, unit>("addblog", "") >> DbCall.Map fst
        
    let spGetBlog = query.Proc<int, unit, Blog>("getblog", "blogId") >> DbCall.Map fst
        
    let insertBlog = query.Sql<Blog, unit>(
        "insert into blog (id, name, title, description, owner, createdAt, modifiedAt, modifiedBy) 
         values (@id, @name, @title, @description, @owner, @createdAt, @modifiedAt, @modifiedBy)")

    let insertBlogAutoInc = query.Sql<Blog, int>(
        "insert into blog (name, title, description, owner, createdAt, modifiedAt, modifiedBy) 
         values (@name, @title, @description, @owner, @createdAt, @modifiedAt, @modifiedBy);
         select last_insert_id()")
    
    let bulkInsertBlogs = bulkCopy.WriteToServer<Blog>()

    let bulkInsertUsers = bulkCopy.WriteToServer(BulkCopyParams.Tuple<string, string, string, byte array>("id", "name", "email", "avatar"), "userprofile")
