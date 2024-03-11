namespace DbFun.Firebird.IntegrationTests

open System.Configuration
open DbFun.Core
open System.Data
open FirebirdSql.Data.FirebirdClient
open DbFun.Core.Builders
open DbFun.Firebird.Builders


module Commons = 

    let connectionString = 
        let config = ConfigurationManager.OpenExeConfiguration(System.Reflection.Assembly.GetExecutingAssembly().Location)
        config.ConnectionStrings.ConnectionStrings.["DbFunTests"].ConnectionString

    let createConnection(): IDbConnection = new FbConnection(connectionString)

    let config = QueryConfig.Default(createConnection)
    let query = QueryBuilder((), config)

    let batch = BatchCommandBuilder()

    let run dbCall = DbCall.Run(createConnection, dbCall)



