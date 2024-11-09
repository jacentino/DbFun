namespace DbFun.MsSql.IntegrationTests

open Microsoft.Data.SqlClient
open System.Configuration
open DbFun.Core
open DbFun.MsSql.Builders
open System.Data

module Commons = 

    let connectionString = 
        let config = ConfigurationManager.OpenExeConfiguration(System.Reflection.Assembly.GetExecutingAssembly().Location)
        config.ConnectionStrings.ConnectionStrings.["DbFunTests"].ConnectionString        

    let createConnection (): IDbConnection = new SqlConnection(connectionString)

    let defaultConfig = QueryConfig.Default(createConnection).UseTvpParams()

    let query = QueryBuilder(defaultConfig)

    let run dbCall = DbCall.Run(createConnection, dbCall)


module Tooling = 
    
    open Commons

    let getNumberOfBlogs = query.Sql<unit, int>("select count(*) from blog")
    
    let deleteAllButFirstBlog = query.Sql<unit, unit>("delete from blog where id > 1")

