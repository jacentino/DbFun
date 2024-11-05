namespace DbFun.OracleManaged.IntegrationTests

open System.Configuration
open DbFun.Core
open DbFun.OracleManaged.Builders
open System.Data
open Oracle.ManagedDataAccess.Client

module Commons = 

    let connectionString = 
        let config = ConfigurationManager.OpenExeConfiguration(System.Reflection.Assembly.GetExecutingAssembly().Location)
        config.ConnectionStrings.ConnectionStrings.["DbFunTests"].ConnectionString

    let createConnection(): IDbConnection = new OracleConnection(connectionString)

    let config = QueryConfig.Default(createConnection).UseOracleArrayParams()

    let query = QueryBuilder(config)

    let run dbCall = DbCall.Run(createConnection, dbCall)



