namespace DbFun.MySql.IntegrationTests

open System.Configuration
open DbFun.Core
open DbFun.Core.Builders
open System.Data
open MySql.Data.MySqlClient

module Commons = 

    let createConnection (): IDbConnection = 
        let config = ConfigurationManager.OpenExeConfiguration(System.Reflection.Assembly.GetExecutingAssembly().Location)
        let connectionString = config.ConnectionStrings.ConnectionStrings.["DbFunTests"].ConnectionString
        new MySqlConnection(connectionString)

    let config = QueryConfig.Default(createConnection)

    let query = QueryBuilder(config)

    let run dbCall = DbCall.Run(createConnection, dbCall)
