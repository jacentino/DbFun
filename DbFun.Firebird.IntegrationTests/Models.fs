namespace DbFun.Firebird.IntegrationTests

open System
open Commons

module Models = 

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

module Tooling = 

    let deleteAllButFirstBlog = 
        query.Sql<unit, unit>("delete from blog where id > 1")




