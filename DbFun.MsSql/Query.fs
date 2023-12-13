namespace DbFun.MsSql.Builders

open DbFun.Core.Builders
open DbFun.Core.Diagnostics
open DbFun.MsSql.Builders
open System.Data
open System

/// <summary>
/// Microsoft SQL Server-specific configuration, including tvp-parameter builders.
/// </summary>
type QueryConfig = 
    {
        Common      : DbFun.Core.Builders.QueryConfig
        TvpBuilders : TableValuedParamsImpl.IBuilder list
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
            {
                Common      = { common with ParamBuilders = ParamsImpl.getDefaultBuilders(createConnection >> unbox) }
                TvpBuilders = TableValuedParamsImpl.getDefaultBuilders()
            }

        /// <summary>
        /// Adds a converter mapping application values of a given type to ptoper database parameter values.
        /// </summary>
        /// <param name="convert">
        /// Function converting application values to database parameter values.
        /// </param>
        member this.AddRowConverter(converter: 'Source -> 'Target) = 
            { this with Common = this.Common.AddRowConverter(converter) }

        /// <summary>
        /// Adds builder for table-valued parameters.
        /// </summary>
        /// <param name="builder">
        /// The builder.
        /// </param>
        member this.AddTvpBuilder(builder: TableValuedParamsImpl.IBuilder) = 
            let tvpBuilders = builder :: this.TvpBuilders
            let tvpProvider = ParamsImpl.BaseSetterProvider(tvpBuilders)
            let tvpCollBuilder = ParamsImpl.TVPCollectionBuilder(this.Common.CreateConnection, tvpProvider) :> ParamsImpl.IBuilder
            let paramBuilders = this.Common.ParamBuilders |> List.map (function :? ParamsImpl.TVPCollectionBuilder -> tvpCollBuilder | b -> b)
            { this with
                Common      = { this.Common with ParamBuilders = paramBuilders }
                TvpBuilders = tvpBuilders
            }

        /// <summary>
        /// Adds a converter mapping database values to application values.
        /// </summary>
        /// <param name="convert">
        /// Function converting database column values to application values.
        /// </param>
        member this.AddParamConverter(converter: 'Source -> 'Target) = 
            let tvpBuilder = ParamsImpl.Converter<'Source, 'Target>(converter) 
            let tvpSeqBuilder = ParamsImpl.SeqItemConverter<'Source, 'Target>(converter) 
            { this with Common = this.Common.AddParamConverter(converter) }
                .AddTvpBuilder(tvpBuilder)
                .AddTvpBuilder(tvpSeqBuilder)

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
                .AddTvpBuilder(ParamsImpl.Configurator<'Config>(getConfig, canBuild))

        /// <summary>
        /// Adds a configurator for row builders of types determined by canBuild function.
        /// </summary>
        /// <param name="getConfig">
        /// Creates a configuration object.
        /// </param>
        /// <param name="canBuild">
        /// Function determining whether a given type is handled by the configurator.
        /// </param>
        member this.AddRowConfigurator(getConfig: string -> 'Config, canBuild: Type -> bool) = 
            { this with Common = this.Common.AddRowConfigurator(getConfig, canBuild) }

        /// <summary>
        /// Adds a configurator for both parameter and row builders of types determined by canBuild function.
        /// </summary>
        /// <param name="getConfig">
        /// Creates a configuration object.
        /// </param>
        /// <param name="canBuild">
        /// Function determining whether a given type is handled by the configurator.
        /// </param>
        member this.AddConfigurator(getConfig: string -> 'Config, canBuild: Type -> bool) = 
            this.AddParamConfigurator(getConfig, canBuild)
                .AddRowConfigurator(getConfig, canBuild)

/// <summary>
/// Provides methods creating various query functions.
/// </summary>
type QueryBuilder(config: QueryConfig, ?compileTimeErrorLog: Ref<CompileTimeErrorLog>) =
    inherit DbFun.Core.Builders.QueryBuilder(config.Common, ?compileTimeErrorLog = compileTimeErrorLog)

    /// <summary>
    /// The configuration of the query builder.
    /// </summary>
    member __.Config = config

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

