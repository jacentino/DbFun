namespace DbFun.MsSql.IntegrationTests

open System
open Commons

module Models = 

    type Comment = {
        id          : int
        parentId    : int option
        content     : string
        author      : string
        createdAt   : DateTime
        replies     : Comment list
    }

    type PostStatus = 
        | New       = 'N'
        | Published = 'P'
        | Archived  = 'A'

    type Post = {
        id          : int
        blogId      : int
        name        : string
        title       : string
        content     : string
        author      : string
        createdAt   : DateTime
        modifiedAt  : DateTime option
        modifiedBy  : string option
        status      : PostStatus
        comments    : Comment list
        tags        : string list
    }

    type Blog = {
        id          : int
        name        : string
        title       : string
        description : string
        owner       : string
        createdAt   : DateTime
        modifiedAt  : DateTime option
        modifiedBy  : string option
        posts       : Post list
    }

    type BlogTZ = {
        id          : int
        name        : string
        title       : string
        description : string
        owner       : string
        createdAt   : DateTimeOffset
        modifiedAt  : DateTimeOffset option
        modifiedBy  : string option
        posts       : Post list
    }

    type SortField = 
        | Name      = 1
        | Title     = 2
        | Author    = 3
        | CreatedAt = 4
        | Status    = 5

    type SortDirection =
        | Asc   = 1
        | Desc  = 2

    type SortOrder = 
        {
            field       : SortField
            direction   : SortDirection
        }
        override this.ToString() = sprintf "%A %A" this.field this.direction

    type Criteria = 
        {
            name        : string option
            title       : string option
            content     : string option
            author      : string option
            createdFrom : DateTime option
            createdTo   : DateTime option
            modifiedFrom: DateTime option
            modifiedTo  : DateTime option
            statuses    : PostStatus list
            tags        : string list
            sortOrder   : SortOrder
        }
        static member Default = 
            {
                name        = None
                title       = None
                content     = None
                author      = None
                createdFrom = None
                createdTo   = None
                modifiedFrom= None
                modifiedTo  = None
                statuses    = []
                tags        = []
                sortOrder   = { field = SortField.CreatedAt; direction = SortDirection.Desc }
            }


module Tooling = 

    let getNumberOfBlogs = query.Sql<unit, int>("select count(*) from blog")
    
    let deleteAllButFirstBlog = query.Sql<unit, unit>("delete from blog where id > 1")

