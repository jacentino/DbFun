namespace DbFun.OracleManaged.IntegrationTests

open System
open Commons

module Models = 

    type Blog = {
        blogId: int
        name: string
        title: string
        description: string
        owner: string
        createdAt: DateTime
        modifiedAt: DateTime option
        modifiedBy: string option
    }

module Tooling = 

    let deleteAllButFirstBlog = 
        query.Sql<unit, unit>("delete from blog where blogid > 1")




