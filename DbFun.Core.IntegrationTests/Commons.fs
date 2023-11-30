namespace DbFun.Core.IntegrationTests

open Microsoft.Data.SqlClient
open System.Configuration
open DbFun.Core
open DbFun.MsSql.Builders

module Commons = 

    let createConnection () = 
        let config = ConfigurationManager.OpenExeConfiguration(System.Reflection.Assembly.GetExecutingAssembly().Location)
        let connectionString = config.ConnectionStrings.ConnectionStrings.["DbFunTests"].ConnectionString
        new SqlConnection(connectionString)

    let defaultConfig = QueryConfig.Default(createConnection >> unbox)

    let query = QueryBuilder(defaultConfig)

    let run (command: IConnector -> Async<'Result>): Async<'Result> = 
        async {
            use connection = createConnection()
            connection.Open()
            let connector = 
                { new IConnector with 
                    member __.Connection = connection
                    member __.Transaction = null
                }
            return! command(connector)        
        }
