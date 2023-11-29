module CITest

open Xunit
open System.Configuration
open Microsoft.Data.SqlClient
open System

[<Fact>]
let testConnection() = 
    let config = ConfigurationManager.OpenExeConfiguration(System.Reflection.Assembly.GetExecutingAssembly().Location)
    let connectionString = config.ConnectionStrings.ConnectionStrings.["CI_TEST"].ConnectionString
    use connection = new SqlConnection(connectionString)
    connection.Open()
    use command = connection.CreateCommand()
    command.CommandText <- "select @@version"
    let ver = command.ExecuteScalar()
    Console.WriteLine("Version: {0}", ver)

