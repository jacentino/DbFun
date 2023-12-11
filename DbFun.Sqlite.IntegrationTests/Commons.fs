namespace DbFun.Sqlite.IntegrationTests

open System.IO
open System.Configuration
open System.Reflection
open System.Data
open System.Data.SQLite
open DbFun.Core
open DbFun.Core.Builders
open DbFun.Core.Sqlite

module Commons = 

    let connectionString = 
        let config = ConfigurationManager.OpenExeConfiguration(System.Reflection.Assembly.GetExecutingAssembly().Location)
        config.ConnectionStrings.ConnectionStrings.["DbFunTests"].ConnectionString
              .Replace("{dir}", Path.GetDirectoryName(Assembly.GetExecutingAssembly().Location))

    let createConnection (): IDbConnection = new SQLiteConnection(connectionString)

    let config = QueryConfig.Default(createConnection).SqliteDateTimeAsString()

    let query = QueryBuilder(config)

    let run dbCall = DbCall.Run(createConnection, dbCall)

