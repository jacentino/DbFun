namespace DbFun.Npgsql.Builders

open System
open System.Data
open DbFun.Core.Diagnostics

type QueryConfig = 
    {
        Common          : DbFun.Core.Builders.QueryConfig
        PgArrayBuilders : PgArrayParamsImpl.IBuilder list
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
            { Common = common; PgArrayBuilders = PgArrayParamsImpl.getDefaultBuilders() }

        /// <summary>
        /// Adds Postgres array support.
        /// </summary>
        member this.UsePostgresArrayParams() = 
            let pgArrayProvider = ParamsImpl.BaseSetterProvider(PgArrayParamsImpl.getDefaultBuilders())
            let pgArrayBuilder = ParamsImpl.PgArrayBuilder(pgArrayProvider) 
            { this with Common = { this.Common with ParamBuilders = pgArrayBuilder :: this.Common.ParamBuilders } }

        /// <summary>
        /// Adds Postgres array support.
        /// </summary>
        member this.UsePostgresArrayResults() = 
            { this with 
                Common = 
                    { this.Common with 
                        RowBuilders = 
                            RowsImpl.ArrayItemConverter<DateTime, DateOnly>(DateOnly.FromDateTime) ::
                            RowsImpl.ArrayItemConverter<TimeSpan, TimeOnly>(TimeOnly.FromTimeSpan) ::
                            RowsImpl.ArrayColumnBuilder() ::
                            RowsImpl.ArrayCollectionConverter() :: 
                            RowsImpl.EnumArrayConverter<int>() ::
                            RowsImpl.EnumArrayConverter<char>() ::
                            RowsImpl.UnionArrayConverter() ::
                            this.Common.RowBuilders 
                    } 
            }

        /// <summary>
        /// Adds Postgres array support.
        /// </summary>
        member this.UsePostgresArrays() = 
            this.UsePostgresArrayParams().UsePostgresArrayResults()

        /// <summary>
        /// Adds a converter mapping application values of a given type to ptoper database parameter values.
        /// </summary>
        /// <param name="convert">
        /// Function converting application values to database parameter values.
        /// </param>
        member this.AddRowConverter(converter: 'Source -> 'Target) = 
            { this with 
                Common = 
                    { this.Common with 
                        RowBuilders = RowsImpl.ArrayItemConverter(converter) :: this.Common.RowBuilders 
                    }.AddRowConverter(converter)
            }

            //.AddRowConverter(converter)

        /// <summary>
        /// Adds builder for table-valued parameters.
        /// </summary>
        /// <param name="builder">
        /// The builder.
        /// </param>
        member this.AddPgArrayBuilder(builder: PgArrayParamsImpl.IBuilder) = 
            let pgArrayBuilders = builder :: this.PgArrayBuilders
            let pgArrayProvider = ParamsImpl.BaseSetterProvider(pgArrayBuilders)
            let arrayBuilder = ParamsImpl.PgArrayBuilder(pgArrayProvider) :> DbFun.Core.Builders.ParamsImpl.IBuilder
            let paramBuilders = this.Common.ParamBuilders |> List.map (function :? ParamsImpl.PgArrayBuilder -> arrayBuilder | b -> b)
            { this with
                Common          = { this.Common with ParamBuilders = paramBuilders }
                PgArrayBuilders = pgArrayBuilders
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
                .AddPgArrayBuilder(arrayBuilder)

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
                .AddPgArrayBuilder(ParamsImpl.Configurator<'Config>(getConfig, canBuild))

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
        /// Allows to handle collections by replicating parameters for each item with name modified by adding item index.
        /// </summary>
        member this.HandleCollectionParams() = 
            { this with Common = this.Common.HandleCollectionParams() }

/// <summary>
/// Provides methods creating various query functions.
/// </summary>
type QueryBuilder(config: QueryConfig, ?compileTimeErrorLog: Ref<CompileTimeErrorLog>) =
    inherit DbFun.Core.Builders.QueryBuilder((), config.Common, ?compileTimeErrorLog = compileTimeErrorLog)

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

    /// <summary>
    /// Allows to handle collections by generating parameters for each item with name modified by adding item index.
    /// </summary>
    member __.HandleCollectionParams() = 
        QueryBuilder(config.HandleCollectionParams(), ?compileTimeErrorLog = compileTimeErrorLog)

    /// <summary>
    /// Handles collections as array parameters.
    /// </summary>
    member __.UsePostgresArrayParamss() = 
        QueryBuilder(config.UsePostgresArrayParams(), ?compileTimeErrorLog = compileTimeErrorLog)

    /// <summary>
    /// Handles collections as array results.
    /// </summary>
    member __.UsePostgresArrayResults() = 
        QueryBuilder(config.UsePostgresArrayResults(), ?compileTimeErrorLog = compileTimeErrorLog)

    /// <summary>
    /// Provides full support for Postgress arrays.
    /// </summary>
    member __.UsePostgresArrays() = 
        QueryBuilder(config.UsePostgresArrays(), ?compileTimeErrorLog = compileTimeErrorLog)