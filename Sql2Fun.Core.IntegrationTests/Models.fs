namespace Sql2Fun.Core.IntegrationTests.Models

open System

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



