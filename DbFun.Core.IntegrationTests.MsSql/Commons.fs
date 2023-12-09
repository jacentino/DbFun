namespace DbFun.Core.IntegrationTests.MsSql

open Microsoft.Data.SqlClient
open System.Configuration
open DbFun.Core
open DbFun.MsSql.Builders
open System.Data

module Commons = 

    let createConnection (): IDbConnection = 
        let config = ConfigurationManager.OpenExeConfiguration(System.Reflection.Assembly.GetExecutingAssembly().Location)
        let connectionString = config.ConnectionStrings.ConnectionStrings.["DbFunTests"].ConnectionString
        new SqlConnection(connectionString)

    let defaultConfig = QueryConfig.Default(createConnection)

    let query = QueryBuilder(defaultConfig)

    let run dbCall = DbCall.Run(createConnection, dbCall)
