namespace DbFun.OracleManaged.IntegrationTests

open System
open DbFun.Core
open DbFun.Core.Builders
open Commons
open Models

module TestQueries = 

    let getBlog = 
        query.Sql<int, Blog>("select blogid, name, title, description, owner, createdAt, modifiedAt, modifiedBy from blog where blogid = :blogid", "blogId")

    let insertBlog =
        query.DisablePrototypeCalls().Sql<Blog, unit>(
            "insert into blog (blogid, name, title, description, owner, createdAt, modifiedAt, modifiedBy) 
             values (:blogId, :name, :title, :description, :owner, :createdAt, :modifiedAt, :modifiedBy)")

    let insertBlogsWithArrays =
        query.DisablePrototypeCalls().Sql<Blog list, unit>(
            "insert into blog (blogid, name, title, description, owner, createdAt) 
             values (:blogid, :name, :title, :description, :owner, :createdAt)")

    let insertBlogProc =
        query.DisablePrototypeCalls().Proc("sp_add_blog", 
            Params.Int("blogId"), 
            Params.Tuple<string, string, string, string, DateTime>("name", "title", "description", "owner", "createdAt"), 
            OutParams.Unit, 
            Results.Unit)
        >> (fun f id -> f id |> DbCall.Map fst)

