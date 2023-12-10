namespace DbFun.Npgsql.IntegrationTests

open System.Configuration
open DbFun.Core
open DbFun.Core.Builders
open DbFun.Npgsql.Builders
open System.Data
open Npgsql

module Commons = 

    let createConnection (): IDbConnection = 
        let config = ConfigurationManager.OpenExeConfiguration(System.Reflection.Assembly.GetExecutingAssembly().Location)
        let connectionString = config.ConnectionStrings.ConnectionStrings.["DbFunTests"].ConnectionString
        new NpgsqlConnection(connectionString)

    let config = QueryConfig.Default(createConnection).UsePostgressArrays()

    let query = QueryBuilder(config)

    let bulkImport = BulkImportBuilder()

    let run dbCall = DbCall.Run(createConnection, dbCall)
