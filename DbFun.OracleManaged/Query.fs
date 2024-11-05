namespace DbFun.OracleManaged.Builders

open DbFun.Core.Diagnostics
open System.Data
open Oracle.ManagedDataAccess.Client
open System

type QueryConfig = 
    {
        Common: DbFun.Core.Builders.QueryConfig
        OracleArrayBuilders : OracleArrayParamsImpl.IBuilder list
    }
    with         
        /// <summary>
        /// Creates default configuration.
        /// </summary>
        /// <param name="createConnection">
        /// The function creating database connection (with proper connection string, but not open).
        /// </param>
        static member Default(createConnection: unit -> IDbConnection) = 
            let common = DbFun.Core.Builders.QueryConfig.Default(createConnection)
            { Common = common; OracleArrayBuilders = OracleArrayParamsImpl.getDefaultBuilders()  }


        /// <summary>
        /// Adds Oracle array support.
        /// </summary>
        member this.UseOracleArrayParams() = 
            let oracleArrayProvider = ParamsImpl.BaseSetterProvider(OracleArrayParamsImpl.getDefaultBuilders())
            let oracleArrayBuilder = ParamsImpl.OracleArrayBuilder(oracleArrayProvider) 
            { this with Common = { this.Common with ParamBuilders = oracleArrayBuilder :: this.Common.ParamBuilders } }


        /// <summary>
        /// Adds builder for array parameters.
        /// </summary>
        /// <param name="builder">
        /// The builder.
        /// </param>
        member this.AddOracleArrayBuilder(builder: OracleArrayParamsImpl.IBuilder) = 
            let pgArrayBuilders = builder :: this.OracleArrayBuilders
            let oracleArrayProvider = ParamsImpl.BaseSetterProvider(pgArrayBuilders)
            let arrayBuilder = ParamsImpl.OracleArrayBuilder(oracleArrayProvider) :> DbFun.Core.Builders.ParamsImpl.IBuilder
            let paramBuilders = this.Common.ParamBuilders |> List.map (function :? ParamsImpl.OracleArrayBuilder -> arrayBuilder | b -> b)
            { this with
                Common              = { this.Common with ParamBuilders = paramBuilders }
                OracleArrayBuilders = pgArrayBuilders
            }

        /// <summary>
        /// Adds a converter mapping database values to application values.
        /// </summary>
        /// <param name="convert">
        /// Function converting database column values to application values.
        /// </param>
        member this.AddParamConverter(converter: 'Source -> 'Target) = 
            let arrayBuilder = ParamsImpl.Converter<'Source, 'Target>(converter) 
            { this with Common = this.Common.AddParamConverter(converter) }
                .AddOracleArrayBuilder(arrayBuilder)

        /// <summary>
        /// Adds a configurator for parameter builders of types determined by canBuild function.
        /// </summary>
        /// <param name="getConfig">
        /// Creates a configuration object.
        /// </param>
        /// <param name="canBuild">
        /// Function determining whether a given type is handled by the configurator.
        /// </param>
        member this.AddParamConfigurator(getConfig: string -> 'Config, canBuild: Type -> bool) = 
            { this with Common = this.Common.AddRowConfigurator(getConfig, canBuild) }
                .AddOracleArrayBuilder(ParamsImpl.Configurator<'Config>(getConfig, canBuild))


        /// <summary>
        /// Allows to handle collections by replicating parameters for each item with name modified by adding item index.
        /// </summary>
        member this.HandleCollectionParams() = 
            { this with Common = this.Common.HandleCollectionParams() }


/// <summary>
/// Provides methods creating various query functions.
/// </summary>
type QueryBuilder(config: QueryConfig, ?compileTimeErrorLog: ref<CompileTimeErrorLog>) =
    inherit DbFun.Core.Builders.QueryBuilder(config.Common, ?compileTimeErrorLog = compileTimeErrorLog)

    /// <summary>
    /// The configuration of the query builder.
    /// </summary>
    member __.Config = config

    override __.CreateCommand(connection: IDbConnection) = 
        let command = connection.CreateCommand()
        (command :?> OracleCommand).BindByName <- true
        command

    /// <summary>
    /// Creates query builder object with default configuration
    /// </summary>
    /// <param name="createConnection">
    /// Function creating connection, assigned with a proper connection string, but not open.
    /// </param>
    new(createConnection: unit -> IDbConnection) = 
        QueryBuilder(QueryConfig.Default(createConnection))

    /// <summary>
    /// Creates new builder with the specified command timeout.
    /// </summary>
    /// <param name="timeout">
    /// The timeout value in seconds.
    /// </param>
    member this.Timeout(timeout: int) = 
        QueryBuilder({ this.Config with Common = { this.Config.Common with Timeout = Some timeout } }, ?compileTimeErrorLog = this.RawCompileTimeErrorLog)

    /// <summary>
    /// Creates new builder with compile-time error logging and deferred exceptions.
    /// </summary>
    member this.LogCompileTimeErrors() = 
        QueryBuilder({ this.Config with Common = { this.Config.Common with LogCompileTimeErrors = true } }, ?compileTimeErrorLog = this.RawCompileTimeErrorLog)

    /// <summary>
    /// Creates new builder generating query functions without discovering resultset structure using SchemaOnly calls.
    /// </summary>
    member this.DisablePrototypeCalls() = 
        QueryBuilder({ this.Config with Common = this.Config.Common.DisablePrototypeCalls() }, ?compileTimeErrorLog = this.RawCompileTimeErrorLog)


    /// <summary>
    /// Handles collections as array parameters.
    /// </summary>
    member __.UseOracleArrayParamss() = 
        QueryBuilder(config.UseOracleArrayParams(), ?compileTimeErrorLog = compileTimeErrorLog)

    /// <summary>
    /// Allows to handle collections by generating parameters for each item with name modified by adding item index.
    /// </summary>
    member __.HandleCollectionParams() = 
        QueryBuilder(config.HandleCollectionParams(), ?compileTimeErrorLog = compileTimeErrorLog)
