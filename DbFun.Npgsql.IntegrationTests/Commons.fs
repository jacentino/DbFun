namespace DbFun.Npgsql.IntegrationTests

open System.Configuration
open DbFun.Core
open DbFun.Npgsql.Builders
open System.Data
open Npgsql

module Commons = 

    let connectionString = 
        let config = ConfigurationManager.OpenExeConfiguration(System.Reflection.Assembly.GetExecutingAssembly().Location)
        config.ConnectionStrings.ConnectionStrings.["DbFunTests"].ConnectionString

    let createConnection (): IDbConnection = new NpgsqlConnection(connectionString)

    let config = QueryConfig.Default(createConnection).UsePostgresArrays()

    let query = QueryBuilder(config)

    let bulkImport = BulkImportBuilder(())

    let run dbCall = DbCall.Run(createConnection, dbCall)
