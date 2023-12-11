namespace DbFun.Sqlite.IntegrationTests

open System
open Commons

module Models = 

    type PostStatus = 
        | New       = 'N'
        | Published = 'P'
        | Archived  = 'A'


    type Blog = {
        id: int
        name: string
        title: string
        description: string
        owner: string
        createdAt: DateTime
        modifiedAt: DateTime option
        modifiedBy: string option
    }

    type Post = {
        id: int
        blogId: int
        name: string
        title: string
        content: string
        author: string
        createdAt: DateTime
        modifiedAt: DateTime option
        modifiedBy: string option
        status: PostStatus
    }



    module Tooling = 

        let deleteAllButFirstBlog = query.Sql<unit, unit>("delete from blog where id <> 1")

        let deleteAllPosts = query.Sql<unit, unit>("delete from post")


