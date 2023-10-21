namespace Sql2Fun.Core.IntegrationTests.Models

open System

type Comment = {
    id: int
    parentId: int option
    content: string
    author: string
    createdAt: DateTime
    replies: Comment list
}

type PostStatus = 
    | New = 'N'
    | Published = 'P'
    | Archived = 'A'

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
    comments: Comment list
    tags: string list
}

type Blog = {
    id: int
    name: string
    title: string
    description: string
    owner: string
    createdAt: DateTime
    modifiedAt: DateTime option
    modifiedBy: string option
    posts: Post list
}




