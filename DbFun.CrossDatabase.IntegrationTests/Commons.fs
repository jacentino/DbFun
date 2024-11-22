namespace DbFun.CrossDatabase.IntegrationTests

open Microsoft.Data.SqlClient
open System.Configuration
open DbFun.Core
open System.Data
open MySql.Data.MySqlClient

module Commons = 

    type Discriminator =
        | MsSqlServer
        | MySql
        | Postgres

    let config = ConfigurationManager.OpenExeConfiguration(System.Reflection.Assembly.GetExecutingAssembly().Location)
    let msSqlConnectionString = config.ConnectionStrings.ConnectionStrings.["MsSqlServer"].ConnectionString        
    let mySqlConnectionString = config.ConnectionStrings.ConnectionStrings.["MySql"].ConnectionString        
    let postgresConnectionString = config.ConnectionStrings.ConnectionStrings.["Postgres"].ConnectionString        

    let createConnection = function 
        | MsSqlServer -> new SqlConnection(msSqlConnectionString) :> IDbConnection
        | MySql       -> new MySqlConnection(mySqlConnectionString)
        | Postgres    -> new Npgsql.NpgsqlConnection(postgresConnectionString)


    let run dbCall = DbCall.Run(createConnection, dbCall)
