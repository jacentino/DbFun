namespace DbFun.CrossDatabase.IntegrationTests

open System

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
