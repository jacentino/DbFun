namespace DbFun.MySqlConnector.IntegrationTests

open System.Configuration
open DbFun.Core
open DbFun.Core.Builders
open System.Data
open MySqlConnector
open DbFun.MySqlConnector

module Commons = 

    let connectionString = 
        let config = ConfigurationManager.OpenExeConfiguration(System.Reflection.Assembly.GetExecutingAssembly().Location)
        config.ConnectionStrings.ConnectionStrings.["DbFunTests"].ConnectionString

    let createConnection (): IDbConnection = 
        new MySqlConnection(connectionString)

    let config = QueryConfig.Default(createConnection)

    let query = QueryBuilder((), config)

    let bulkCopy = BulkCopyBuilder(())

    let run dbCall = DbCall.Run(createConnection, dbCall)
