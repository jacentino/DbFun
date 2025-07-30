﻿namespace DbFun.MsSql.Builders

open DbFun.Core.Builders
open DbFun.Core.Diagnostics
open DbFun.MsSql.Builders
open System.Data
open System
open Microsoft.Data.SqlClient.Server
open DbFun.Core.Builders.Compilers

/// <summary>
/// Microsoft SQL Server-specific configuration, including tvp-parameter builders.
/// </summary>
type QueryConfig<'DbKey> = 
    {
        Common      : DbFun.Core.Builders.QueryConfig<'DbKey>
        TvpBuilders : TableValuedParamsImpl.IBuilder list
    }
    with 
        /// <summary>
        /// Creates default configuration.
        /// </summary>
        /// <param name="createConnection">
        /// The function creating database connection (with proper connection string, but not open).
        /// </param>
        static member Default(createConnection: 'DbKey -> IDbConnection) = 
            let common = DbFun.Core.Builders.QueryConfig<'DbKey>.Default(createConnection)
            {
                Common      = { common with ParamBuilders = ParamsImpl.getDefaultBuilders() }
                TvpBuilders = TableValuedParamsImpl.getDefaultBuilders()
            }
        //static member Default(createConnection: unit -> IDbConnection) = 
        //    let common = DbFun.Core.Builders.QueryConfig.Default(createConnection)
        //    { Common = common; TvpBuilders = TableValuedParamsImpl.getDefaultBuilders() }

        /// <summary>
        /// Handles collections of compound types (records, tuples) as table-valued parameters.
        /// </summary>
        member this.UseTvpParams() = 
            let tvpProvider = GenericSetters.BaseSetterProvider<SqlDataRecord, SqlDataRecord>(TableValuedParamsImpl.getDefaultBuilders(), LinqExpressionCompiler())
            let tvpBuilder = ParamsImpl.TVPCollectionBuilder(tvpProvider) 
            { this with Common = { this.Common with ParamBuilders = tvpBuilder :: this.Common.ParamBuilders } }
        /// <summary>
        /// Adds builder for table-valued parameters.
        /// </summary>
        /// <param name="builder">
        /// The builder.
        /// </param>
        member this.AddTvpBuilder(builder: TableValuedParamsImpl.IBuilder) = 
            let tvpBuilders = builder :: this.TvpBuilders
            let tvpProvider = ParamsImpl.BaseSetterProvider(tvpBuilders, LinqExpressionCompiler())
            let tvpCollBuilder = ParamsImpl.TVPCollectionBuilder(tvpProvider) :> ParamsImpl.IBuilder
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
            { this with Common = this.Common.AddParamConverter(converter) }
                .AddTvpBuilder(tvpBuilder)

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


        interface IDerivedConfig<QueryConfig<'DbKey>, 'DbKey> with
            member this.MapCommon(map: DbFun.Core.Builders.QueryConfig<'DbKey> -> DbFun.Core.Builders.QueryConfig<'DbKey>): QueryConfig<'DbKey> = 
                { this with Common = map(this.Common) }


/// <summary>
/// Microsoft SQL Server-specific configuration, including tvp-parameter builders.
/// </summary>
type QueryConfig = QueryConfig<unit>


/// <summary>
/// Provides methods creating various query functions.
/// </summary>
type QueryBuilder<'DbKey>(dbKey: 'DbKey, config: QueryConfig<'DbKey>, ?compileTimeErrorLog: Ref<CompileTimeErrorLog>) =
    inherit DbFun.Core.Builders.QueryBuilder<'DbKey>(dbKey, config.Common, ?compileTimeErrorLog = compileTimeErrorLog)

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
    new(dbKey: 'DbKey, createConnection: 'DbKey -> IDbConnection) = 
        QueryBuilder<'DbKey>(dbKey, QueryConfig<'DbKey>.Default(createConnection))

    /// <summary>
    /// Creates new builder with the specified command timeout.
    /// </summary>
    /// <param name="timeout">
    /// The timeout value in seconds.
    /// </param>
    member this.Timeout(timeout: int) = 
        QueryBuilder<'DbKey>(dbKey, { this.Config with Common = { this.Config.Common with Timeout = Some timeout } }, ?compileTimeErrorLog = this.RawCompileTimeErrorLog)

    /// <summary>
    /// Creates new builder with compile-time error logging and deferred exceptions.
    /// </summary>
    member this.LogCompileTimeErrors() = 
        QueryBuilder<'DbKey>(dbKey, { this.Config with Common = { this.Config.Common with LogCompileTimeErrors = true } }, ?compileTimeErrorLog = this.RawCompileTimeErrorLog)

    /// <summary>
    /// Creates new builder generating query functions without discovering resultset structure using SchemaOnly calls.
    /// </summary>
    member this.DisablePrototypeCalls() = 
        QueryBuilder<'DbKey>(dbKey, { this.Config with Common = this.Config.Common.DisablePrototypeCalls() }, ?compileTimeErrorLog = this.RawCompileTimeErrorLog)


    /// <summary>
    /// Allows to handle collections by generating parameters for each item with name modified by adding item index.
    /// </summary>
    member __.HandleCollectionParams() = 
        QueryBuilder<'DbKey>(dbKey, config.HandleCollectionParams(), ?compileTimeErrorLog = compileTimeErrorLog)

    /// <summary>
    /// Handles collections of compound types (records, tuples) as table-valued parameters.
    /// </summary>
    member __.UseTvpParams() = 
        QueryBuilder<'DbKey>(dbKey, config.UseTvpParams(), ?compileTimeErrorLog = compileTimeErrorLog)


/// <summary>
/// Provides methods creating various query functions.
/// </summary>
type QueryBuilder(config: QueryConfig, ?compileTimeErrorLog: Ref<CompileTimeErrorLog>) =
    inherit QueryBuilder<unit>((), config, ?compileTimeErrorLog = compileTimeErrorLog)

    /// <summary>
    /// Creates query builder object with default configuration
    /// </summary>
    /// <param name="createConnection">
    /// Function creating connection, assigned with a proper connection string, but not open.
    /// </param>
    new(createConnection: unit -> IDbConnection) = 
        QueryBuilder(QueryConfig.Default(createConnection))
