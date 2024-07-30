namespace DbFun.Core.Builders

open System.Data
open DbFun.Core
open DbFun.Core.Diagnostics
open System.Runtime.CompilerServices
open System.Runtime.InteropServices
open System
open System.Data.Common

/// <summary>
/// The query builder configuration data.
/// </summary>
type QueryConfig = 
    {
        CreateConnection    : unit -> IDbConnection
        ParamBuilders       : ParamsImpl.IBuilder list
        OutParamBuilders    : OutParamsImpl.IBuilder list
        RowBuilders         : RowsImpl.IBuilder list
        Timeout             : int option
        LogCompileTimeErrors: bool
        PrototypeCalls      : bool
    }
    with 
        /// <summary>
        /// Creates default configuration.
        /// </summary>
        /// <param name="createConnection">
        /// The function creating database connection (with proper connection string, but not open).
        /// </param>
        static member Default(createConnection): QueryConfig = 
            {
                CreateConnection    = createConnection
                ParamBuilders       = ParamsImpl.getDefaultBuilders()
                OutParamBuilders    = OutParamsImpl.getDefaultBuilders()
                RowBuilders         = RowsImpl.getDefaultBuilders()
                Timeout             = None
                LogCompileTimeErrors= false
                PrototypeCalls      = true
            }

        /// <summary>
        /// Adds a converter mapping application values of a given type to ptoper database parameter values.
        /// </summary>
        /// <param name="convert">
        /// Function converting application values to database parameter values.
        /// </param>
        member this.AddParamConverter(convert: 'Source -> 'Target) = 
            { this with ParamBuilders = 
                            ParamsImpl.Converter<'Source, 'Target>(convert) :: 
                            this.ParamBuilders 
            }

        /// <summary>
        /// Adds a converter mapping database values to application values.
        /// </summary>
        /// <param name="convert">
        /// Function converting database column values to application values.
        /// </param>
        member this.AddRowConverter(convert: 'Source -> 'Target) = 
            { this with 
                RowBuilders = RowsImpl.Converter<'Source, 'Target>(convert) :: this.RowBuilders
                OutParamBuilders = OutParamsImpl.Converter<'Source, 'Target>(convert) ::  this.OutParamBuilders
            }

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
            { this with ParamBuilders = ParamsImpl.Configurator<'Config>(getConfig, canBuild) :: this.ParamBuilders }

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
            { this with 
                RowBuilders = RowsImpl.Configurator<'Config>(getConfig, canBuild) :: this.RowBuilders 
                OutParamBuilders = OutParamsImpl.Configurator<'Config>(getConfig, canBuild) :: this.OutParamBuilders
            }

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
        /// Allows to generate functions executing queries without discovering resultset structure using SchemaOnly calls.
        /// </summary>
        member this.DisablePrototypeCalls() = 
            { this with 
                PrototypeCalls = false
                RowBuilders = RowsImpl.NoPrototypeColumnBuilder() :: this.RowBuilders
            }

        /// <summary>
        /// Allows to handle collections by replicating parameters for each item with name modified by adding item index.
        /// </summary>
        member this.HandleCollectionParams() = 
            { this with ParamBuilders = ParamsImpl.SequenceIndexingBuilder() :: this.ParamBuilders }

/// <summary>
/// Provides methods creating various query functions.
/// </summary>
type QueryBuilder(config: QueryConfig, ?compileTimeErrorLog: ref<CompileTimeErrorLog>) =

    let compileTimeErrorLog = 
        if config.LogCompileTimeErrors then 
            compileTimeErrorLog |> Option.orElse (Some (ref<CompileTimeErrorLog> []))
        else 
            None

    let executeReaderAsync (command: IDbCommand, behavior: CommandBehavior): Async<IDataReader> = 
        async {
            match command with 
            | :? DbCommand as dbCommand ->
                let! token = Async.CancellationToken
                let! reader = dbCommand.ExecuteReaderAsync(behavior, token) |> Async.AwaitTask
                return reader
            | _ ->
                let reader = command.ExecuteReader(behavior) 
                return reader
        }


    let executePrototypeQuery(commandType: CommandType, commandText: string, setParams: IDbCommand -> unit, resultReaderBuilder: IDataReader -> IResultReader<'Result>) =
        if config.PrototypeCalls then
            use connection = config.CreateConnection()
            connection.Open()
            use transaction = connection.BeginTransaction()
            use command = connection.CreateCommand()
            command.CommandType <- commandType
            command.CommandText <- commandText
            command.Transaction <- transaction
            setParams(command)
            use prototype = command.ExecuteReader(CommandBehavior.SchemaOnly)
            resultReaderBuilder(prototype)
        else
            resultReaderBuilder(null)

    let executeQuery (provider: IConnector, commandText: string, resultReader: IResultReader<'Result>, setParams: IDbCommand -> unit) = 
        async {
            use command = provider.Connection.CreateCommand()
            command.CommandType <- CommandType.Text
            command.CommandText <- commandText
            command.Transaction <- provider.Transaction
            match config.Timeout with
            | Some timeout -> command.CommandTimeout <- timeout
            | None -> ()
            setParams(command)
            use! dataReader = executeReaderAsync(command, CommandBehavior.Default)
            return! resultReader.Read(dataReader)
        }

    let executeProcedure (provider: IConnector, commandText: string, outParamGetter: IOutParamGetter<'OutParams>, resultReader: IResultReader<'Result>, setParams: IDbCommand -> unit) = 
        async {
            use command = provider.Connection.CreateCommand()
            command.CommandType <- CommandType.StoredProcedure
            command.CommandText <- commandText
            command.Transaction <- provider.Transaction
            match config.Timeout with
            | Some timeout -> command.CommandTimeout <- timeout
            | None -> ()
            setParams(command)
            outParamGetter.Create(command)
            use! dataReader = executeReaderAsync(command, CommandBehavior.Default) 
            let! result = resultReader.Read(dataReader)
            return result, outParamGetter.Get(command)
        }

    let handleException (sourcePath: string, sourceLine: int, ex: exn) = 
        match compileTimeErrorLog with
        | Some errorLog -> 
            errorLog.Value <- (sourceLine, sourcePath, ex) :: errorLog.Value
            fun _ -> 
                raise <| AggregateException("One or more exceptions occured when compiling queries.", 
                            errorLog.Value 
                            |> List.rev
                            |> List.map (fun (line, source, ex) -> CompileTimeException(sprintf "Cannot compile query in %s, line: %d" source line, ex) :> exn))
        | None ->
            raise <| CompileTimeException(sprintf "Cannot compile query in %s, line: %d" sourcePath sourceLine, ex)

    /// <summary>
    /// Creates query builder object with default configuration
    /// </summary>
    /// <param name="createConnection">
    /// Function creating connection, assigned with a proper connection string, but not open.
    /// </param>
    new(createConnection: unit -> IDbConnection) = 
        QueryBuilder(QueryConfig.Default(createConnection))

    /// <summary>
    /// The configuration of the query builder.
    /// </summary>
    member __.Config = config

    /// <summary>
    /// Creates new builder with the specified command timeout.
    /// </summary>
    /// <param name="timeout">
    /// The timeout value in seconds.
    /// </param>
    member __.Timeout(timeout: int) = 
        QueryBuilder({ config with Timeout = Some timeout }, ?compileTimeErrorLog = compileTimeErrorLog)

    /// <summary>
    /// Creates new builder with compile-time error logging and deferred exceptions.
    /// </summary>
    member __.LogCompileTimeErrors() = 
        QueryBuilder({ config with LogCompileTimeErrors = true }, ?compileTimeErrorLog = compileTimeErrorLog)

    /// <summary>
    /// Creates new builder generating query functions without discovering resultset structure using SchemaOnly calls.
    /// </summary>
    member __.DisablePrototypeCalls() = 
        QueryBuilder(config.DisablePrototypeCalls(), ?compileTimeErrorLog = compileTimeErrorLog)

    /// <summary>
    /// Allows to handle collections by generating parameters for each item with name modified by adding item index.
    /// </summary>
    member __.HandleCollectionParams() = 
        QueryBuilder(config.HandleCollectionParams(), ?compileTimeErrorLog = compileTimeErrorLog)

    /// <summary>
    /// The list of compile time errors.
    /// </summary>
    member __.CompileTimeErrors = 
        match compileTimeErrorLog with
        | Some log -> log.Value |> List.rev
        | None -> []

    member __.RawCompileTimeErrorLog = compileTimeErrorLog 

    /// <summary>
    /// Builds a one arg query function based on command template.
    /// </summary>
    /// <param name="template">
    /// The template expansion function.
    /// </param>
    /// <param name="paramSpecifier">
    /// The parameter builder.
    /// </param>
    /// <param name="resultSpecifier">
    /// The result reader builder.
    /// </param>
    /// <param name="sourcePath">
    /// The calling source path for diagnostic purposes.
    /// </param>
    /// <param name="sourceLine">
    /// The calling source path for diagnostic purposes.
    /// </param>
    member __.Sql (
            template: 'Params option -> string,
            paramSpecifier: ParamSpecifier<'Params>, 
            resultSpecifier: ResultSpecifier<'Result>,
            [<CallerFilePath; Optional; DefaultParameterValue("")>] sourcePath: string,
            [<CallerLineNumber; Optional; DefaultParameterValue(0)>] sourceLine: int)
            : 'Params -> DbCall<'Result> =         
        try
            let provider = GenericSetters.BaseSetterProvider<unit, IDbCommand>(config.ParamBuilders)
            let paramSetter = paramSpecifier(provider, ())

            let rowGetterProvider = GenericGetters.BaseGetterProvider<IDataRecord, IDataRecord>(config.RowBuilders)
                       
            let resultSpecifier' prototype = resultSpecifier(rowGetterProvider, prototype)

            let resultReader = executePrototypeQuery(CommandType.Text, template(None), (fun cmd -> paramSetter.SetArtificial(None, cmd)), resultSpecifier')

            fun (parameters: 'Params) (provider: IConnector) ->
                executeQuery(provider, template(Some parameters), resultReader, fun cmd -> paramSetter.SetValue(parameters, None, cmd))
        with ex ->
            handleException(sourcePath, sourceLine, ex)

    /// <summary>
    /// Builds a one arg query function based on command template.
    /// </summary>
    /// <param name="template">
    /// The template expansion function.
    /// </param>
    /// <param name="name">
    /// The parameter name.
    /// </param>
    /// <param name="resultSpecifier">
    /// The result reader builder.
    /// </param>
    /// <param name="sourcePath">
    /// The calling source path for diagnostic purposes.
    /// </param>
    /// <param name="sourceLine">
    /// The calling source path for diagnostic purposes.
    /// </param>
    member this.Sql (
            template: 'Params option -> string,
            name: string, 
            resultSpecifier: ResultSpecifier<'Result>,
            [<CallerFilePath; Optional; DefaultParameterValue("")>] sourcePath: string,
            [<CallerLineNumber; Optional; DefaultParameterValue(0)>] sourceLine: int)
            : 'Params -> DbCall<'Result> =         
        this.Sql(template, Params.Auto<'Params>(name), resultSpecifier, sourcePath, sourceLine)

    /// <summary>
    /// Builds a one arg query function based on command template.
    /// </summary>
    /// <param name="template">
    /// The template expansion function.
    /// </param>
    /// <param name="name">
    /// The parameter name.
    /// </param>
    /// <param name="resultName">
    /// The result column name.
    /// </param>
    /// <param name="sourcePath">
    /// The calling source path for diagnostic purposes.
    /// </param>
    /// <param name="sourceLine">
    /// The calling source path for diagnostic purposes.
    /// </param>
    member this.Sql (
            template: 'Params option -> string,
            [<Optional>] name: string, 
            [<Optional>] resultName: string,
            [<CallerFilePath; Optional; DefaultParameterValue("")>] sourcePath: string,
            [<CallerLineNumber; Optional; DefaultParameterValue(0)>] sourceLine: int)
            : 'Params -> DbCall<'Result> =         
        this.Sql(template, Params.Auto<'Params>(name), Results.Auto(resultName), sourcePath, sourceLine)

    /// <summary>
    /// Builds a one arg query function based on raw SQL text.
    /// </summary>
    /// <param name="commandText">
    /// The SQL command text.
    /// </param>
    /// <param name="paramSpecifier">
    /// The parameter builder.
    /// </param>
    /// <param name="sourcePath">
    /// The calling source path for diagnostic purposes.
    /// </param>
    /// <param name="sourceLine">
    /// The calling source path for diagnostic purposes.
    /// </param>
    /// <param name="resultSpecifier">
    /// The result builder.
    /// </param>
    member this.Sql(
            commandText: string,
            paramSpecifier: ParamSpecifier<'Params>, 
            resultSpecifier: ResultSpecifier<'Result>,
            [<CallerFilePath; Optional; DefaultParameterValue("")>] sourcePath: string,
            [<CallerLineNumber; Optional; DefaultParameterValue(0)>] sourceLine: int)
            : 'Params -> DbCall<'Result> =         
            this.Sql((fun _ -> commandText), paramSpecifier, resultSpecifier, sourcePath, sourceLine) 

    /// <summary>
    /// Builds a one arg query function based on raw SQL text.
    /// </summary>
    /// <param name="commandText">
    /// The SQL command text.
    /// </param>
    /// <param name="name">
    /// The parameter name.
    /// </param>
    /// <param name="sourcePath">
    /// The calling source path for diagnostic purposes.
    /// </param>
    /// <param name="sourceLine">
    /// The calling source line for diagnostic purposes.
    /// </param>
    /// <param name="resultSpecifier">
    /// The result builder.
    /// </param>
    member this.Sql<'Params, 'Result> (
            commandText: string,
            name: string, resultSpecifier: ResultSpecifier<'Result>,
            [<CallerFilePath; Optional; DefaultParameterValue("")>] sourcePath: string,
            [<CallerLineNumber; Optional; DefaultParameterValue(0)>] sourceLine: int)
            : 'Params -> DbCall<'Result> =         
        this.Sql(commandText, Params.Auto<'Params>(name), resultSpecifier, sourcePath, sourceLine) 

    /// <summary>
    /// Builds a one arg query function based on raw SQL text.
    /// </summary>
    /// <param name="commandText">
    /// The SQL command text.
    /// </param>
    /// <param name="argName">
    /// The parameter name.
    /// </param>
    /// <param name="resultName">
    /// The result column name.
    /// </param>
    /// <param name="sourcePath">
    /// The calling source path for diagnostic purposes.
    /// </param>
    /// <param name="sourceLine">
    /// The calling source line for diagnostic purposes.
    /// </param>
    member this.Sql<'Params, 'Result> (
            commandText: string,
            [<Optional>] argName: string, [<Optional>] resultName: string,
            [<CallerFilePath; Optional; DefaultParameterValue("")>] sourcePath: string,
            [<CallerLineNumber; Optional; DefaultParameterValue(0)>] sourceLine: int)
            : 'Params -> DbCall<'Result> =         
        this.Sql<'Params, 'Result>(commandText, argName, Results.Auto<'Result>(resultName), sourcePath, sourceLine) 

    /// <summary>
    /// Builds a query function with two curried args based on a command template.
    /// </summary>
    /// <param name="template">
    /// The SQL command template.
    /// </param>
    /// <param name="paramSpecifier1">
    /// The first parameter builder.
    /// </param>
    /// <param name="paramSpecifier2">
    /// The second parameter builder.
    /// </param>
    /// <param name="sourcePath">
    /// The calling source path for diagnostic purposes.
    /// </param>
    /// <param name="sourceLine">
    /// The calling source path for diagnostic purposes.
    /// </param>
    /// <param name="resultSpecifier">
    /// The result builder.
    /// </param>
    member __.Sql (
        template: ('Params1 * 'Params2) option -> string,
        paramSpecifier1: ParamSpecifier<'Params1>, 
        paramSpecifier2: ParamSpecifier<'Params2>,
        resultSpecifier: ResultSpecifier<'Result>,
        [<CallerFilePath; Optional; DefaultParameterValue("")>] sourcePath: string,
        [<CallerLineNumber; Optional; DefaultParameterValue(0)>] sourceLine: int)
        : 'Params1 -> 'Params2 -> DbCall<'Result> = 
            try                        
                let provider = GenericSetters.BaseSetterProvider<unit, IDbCommand>(config.ParamBuilders)
                let paramSetter1 = paramSpecifier1(provider, ())
                let paramSetter2 = paramSpecifier2(provider, ())

                let rowGetterProvider = GenericGetters.BaseGetterProvider<IDataRecord, IDataRecord>(config.RowBuilders)
                let resultSpecifier' prototype = resultSpecifier(rowGetterProvider, prototype)

                let setArtificial(command: IDbCommand) = 
                    paramSetter1.SetArtificial(None, command)
                    paramSetter2.SetArtificial(None, command)

                let resultReader = executePrototypeQuery(CommandType.Text, template(None), setArtificial, resultSpecifier')

                let setParams (parameters1: 'Params1, parameters2: 'Params2) (command: IDbCommand) = 
                    paramSetter1.SetValue(parameters1, None, command) 
                    paramSetter2.SetValue(parameters2, None, command)

                fun (parameters1: 'Params1) (parameters2: 'Params2) (provider: IConnector) ->
                    executeQuery(provider, template(Some (parameters1, parameters2)), resultReader, setParams(parameters1, parameters2))
            with ex ->
                handleException(sourcePath, sourceLine, ex)

    /// <summary>
    /// Builds a query function with two curried args based on a command template.
    /// </summary>
    /// <param name="template">
    /// The SQL command template.
    /// </param>
    /// <param name="name1">
    /// The first parameter name.
    /// </param>
    /// <param name="name2">
    /// The second parameter name.
    /// </param>
    /// <param name="sourcePath">
    /// The calling source path for diagnostic purposes.
    /// </param>
    /// <param name="sourceLine">
    /// The calling source path for diagnostic purposes.
    /// </param>
    /// <param name="resultSpecifier">
    /// The result builder.
    /// </param>
    member this.Sql<'Params1, 'Params2, 'Result> (
            template: ('Params1 * 'Params2) option -> string,
            name1: string, name2: string, 
            resultSpecifier: ResultSpecifier<'Result>,
            [<CallerFilePath; Optional; DefaultParameterValue("")>] sourcePath: string,
            [<CallerLineNumber; Optional; DefaultParameterValue(0)>] sourceLine: int)
            : 'Params1 -> 'Params2 -> DbCall<'Result> =         
        this.Sql(template, Params.Auto<'Params1>(name1), Params.Auto<'Params2>(name2), resultSpecifier, sourcePath, sourceLine)

    /// <summary>
    /// Builds a query function with two curried args based on a command template.
    /// </summary>
    /// <param name="template">
    /// The SQL command template.
    /// </param>
    /// <param name="name1">
    /// The first parameter name.
    /// </param>
    /// <param name="name2">
    /// The second parameter name.
    /// </param>
    /// <param name="sourcePath">
    /// The calling source path for diagnostic purposes.
    /// </param>
    /// <param name="sourceLine">
    /// The calling source path for diagnostic purposes.
    /// </param>
    /// <param name="resultName">
    /// The result column name or prefix.
    /// </param>
    member this.Sql<'Params1, 'Params2, 'Result> (
            template: ('Params1 * 'Params2) option -> string,
            [<Optional>] name1: string, [<Optional>] name2: string, [<Optional>] resultName: string,
            [<CallerFilePath; Optional; DefaultParameterValue("")>] sourcePath: string,
            [<CallerLineNumber; Optional; DefaultParameterValue(0)>] sourceLine: int)
            : 'Params1 -> 'Params2 -> DbCall<'Result> =         
        this.Sql(template, Params.Auto<'Params1>(name1), Params.Auto<'Params2>(name2), Results.Auto<'Result>(resultName), sourcePath, sourceLine)

    /// <summary>
    /// Builds a query function with two curried args based on raw SQL text.
    /// </summary>
    /// <param name="commandText">
    /// The SQL command text.
    /// </param>
    /// <param name="paramSpecifier1">
    /// The first parameter builder.
    /// </param>
    /// <param name="paramSpecifier2">
    /// The second parameter builder.
    /// </param>
    /// <param name="sourcePath">
    /// The calling source path for diagnostic purposes.
    /// </param>
    /// <param name="sourceLine">
    /// The calling source path for diagnostic purposes.
    /// </param>
    /// <param name="resultSpecifier">
    /// The result builder.
    /// </param>
    member this.Sql (
        commandText: string,
        paramSpecifier1: ParamSpecifier<'Params1>, 
        paramSpecifier2: ParamSpecifier<'Params2>,
        resultSpecifier: ResultSpecifier<'Result>,
        [<CallerFilePath; Optional; DefaultParameterValue("")>] sourcePath: string,
        [<CallerLineNumber; Optional; DefaultParameterValue(0)>] sourceLine: int)
        : 'Params1 -> 'Params2 -> DbCall<'Result> = 
            this.Sql((fun _ -> commandText), paramSpecifier1, paramSpecifier2, resultSpecifier, sourcePath, sourceLine)

    /// <summary>
    /// Builds a query function with two curried args based on raw SQL text.
    /// </summary>
    /// <param name="commandText">
    /// The SQL command text.
    /// </param>
    /// <param name="name1">
    /// The first parameter name.
    /// </param>
    /// <param name="name2">
    /// The second parameter name.
    /// </param>
    /// <param name="sourcePath">
    /// The calling source path for diagnostic purposes.
    /// </param>
    /// <param name="sourceLine">
    /// The calling source path for diagnostic purposes.
    /// </param>
    /// <param name="resultSpecifier">
    /// The result builder.
    /// </param>
    member this.Sql<'Params1, 'Params2, 'Result> (
            commandText: string,
            name1: string, name2: string, 
            resultSpecifier: ResultSpecifier<'Result>,
            [<CallerFilePath; Optional; DefaultParameterValue("")>] sourcePath: string,
            [<CallerLineNumber; Optional; DefaultParameterValue(0)>] sourceLine: int)
            : 'Params1 -> 'Params2 -> DbCall<'Result> =         
        this.Sql(commandText, Params.Auto<'Params1>(name1), Params.Auto<'Params2>(name2), resultSpecifier, sourcePath, sourceLine)

    /// <summary>
    /// Builds a query function with three curried args based on a command template.
    /// </summary>
    /// <param name="template">
    /// The SQL command template.
    /// </param>
    /// <param name="paramSpecifier1">
    /// The first parameter builder.
    /// </param>
    /// <param name="paramSpecifier2">
    /// The second parameter builder.
    /// </param>
    /// <param name="paramSpecifier3">
    /// The third parameter builder.
    /// </param>
    /// <param name="sourcePath">
    /// The calling source path for diagnostic purposes.
    /// </param>
    /// <param name="sourceLine">
    /// The calling source path for diagnostic purposes.
    /// </param>
    /// <param name="resultSpecifier">
    /// The result builder.
    /// </param>
    member __.Sql (
            template: ('Params1 * 'Params2 * 'Params3) option ->  string,
            paramSpecifier1: ParamSpecifier<'Params1>, 
            paramSpecifier2: ParamSpecifier<'Params2>,
            paramSpecifier3: ParamSpecifier<'Params3>,
            resultSpecifier: ResultSpecifier<'Result>,
            [<CallerFilePath; Optional; DefaultParameterValue("")>] sourcePath: string,
            [<CallerLineNumber; Optional; DefaultParameterValue(0)>] sourceLine: int)
            : 'Params1 -> 'Params2 -> 'Params3 -> DbCall<'Result> = 
        try                        
            let provider = GenericSetters.BaseSetterProvider<unit, IDbCommand>(config.ParamBuilders)
            let paramSetter1 = paramSpecifier1(provider, ())
            let paramSetter2 = paramSpecifier2(provider, ())
            let paramSetter3 = paramSpecifier3(provider, ())

            let rowGetterProvider = GenericGetters.BaseGetterProvider<IDataRecord, IDataRecord>(config.RowBuilders)
            let resultSpecifier' prototype = resultSpecifier(rowGetterProvider, prototype)

            let setArtificial(command: IDbCommand) = 
                paramSetter1.SetArtificial(None, command)
                paramSetter2.SetArtificial(None, command)
                paramSetter3.SetArtificial(None, command)                    

            let resultReader = executePrototypeQuery(CommandType.Text, template(None), setArtificial, resultSpecifier')

            let setParams (parameters1: 'Params1, parameters2: 'Params2, parameters3: 'Params3) (command: IDbCommand) = 
                paramSetter1.SetValue(parameters1, None, command) 
                paramSetter2.SetValue(parameters2, None, command)
                paramSetter3.SetValue(parameters3, None, command)

            fun (parameters1: 'Params1) (parameters2: 'Params2) (parameters3: 'Params3) (provider: IConnector) ->
                executeQuery(provider, template(Some (parameters1, parameters2, parameters3)), resultReader, setParams(parameters1, parameters2, parameters3))
        with ex ->
            handleException(sourcePath, sourceLine, ex)

    /// <summary>
    /// Builds a query function with three curried args based on a command template.
    /// </summary>
    /// <param name="template">
    /// The SQL command text.
    /// </param>
    /// <param name="name1">
    /// The first parameter name.
    /// </param>
    /// <param name="name2">
    /// The second parameter name.
    /// </param>
    /// <param name="name3">
    /// The third parameter name.
    /// </param>
    /// <param name="sourcePath">
    /// The calling source path for diagnostic purposes.
    /// </param>
    /// <param name="sourceLine">
    /// The calling source path for diagnostic purposes.
    /// </param>
    /// <param name="resultSpecifier">
    /// The result builder.
    /// </param>
    member this.Sql<'Params1, 'Params2, 'Params3, 'Result> (
            template: ('Params1 * 'Params2 * 'Params3) option ->  string,
            name1: string, name2: string, name3: string, 
            resultSpecifier: ResultSpecifier<'Result>,
            [<CallerFilePath; Optional; DefaultParameterValue("")>] sourcePath: string,
            [<CallerLineNumber; Optional; DefaultParameterValue(0)>] sourceLine: int)
            : 'Params1 -> 'Params2 -> 'Params3 -> DbCall<'Result> =         
        this.Sql(template, Params.Auto<'Params1>(name1), Params.Auto<'Params2>(name2), Params.Auto<'Params3>(name3), resultSpecifier, sourcePath, sourceLine)

    /// <summary>
    /// Builds a query function with three curried args based on a command template.
    /// </summary>
    /// <param name="template">
    /// The SQL command template.
    /// </param>
    /// <param name="argName1">
    /// The first parameter name.
    /// </param>
    /// <param name="argName2">
    /// The second parameter name.
    /// </param>
    /// <param name="argName3">
    /// The third parameter name.
    /// </param>
    /// <param name="resultName">
    /// The result column name.
    /// </param>
    /// <param name="sourcePath">
    /// The calling source path for diagnostic purposes.
    /// </param>
    /// <param name="sourceLine">
    /// The calling source path for diagnostic purposes.
    /// </param>
    member this.Sql<'Params1, 'Params2, 'Params3, 'Result> (
            template: ('Params1 * 'Params2 * 'Params3) option ->  string,
            [<Optional>] argName1: string, [<Optional>] argName2: string, [<Optional>] argName3: string, [<Optional>] resultName: string,
            [<CallerFilePath; Optional; DefaultParameterValue("")>] sourcePath: string,
            [<CallerLineNumber; Optional; DefaultParameterValue(0)>] sourceLine: int)
            : 'Params1 -> 'Params2 -> 'Params3 -> DbCall<'Result> =         
        this.Sql(template, Params.Auto<'Params1>(argName1), Params.Auto<'Params2>(argName2), Params.Auto<'Params3>(argName3), Results.Auto<'Result>(resultName), sourcePath, sourceLine) 


    /// <summary>
    /// Builds a query function with three curried args based on raw SQL text.
    /// </summary>
    /// <param name="commandText">
    /// The SQL command text.
    /// </param>
    /// <param name="paramSpecifier1">
    /// The first parameter builder.
    /// </param>
    /// <param name="paramSpecifier2">
    /// The second parameter builder.
    /// </param>
    /// <param name="paramSpecifier3">
    /// The third parameter builder.
    /// </param>
    /// <param name="sourcePath">
    /// The calling source path for diagnostic purposes.
    /// </param>
    /// <param name="sourceLine">
    /// The calling source path for diagnostic purposes.
    /// </param>
    /// <param name="resultSpecifier">
    /// The result builder.
    /// </param>
    member this.Sql (
            commandText: string,
            paramSpecifier1: ParamSpecifier<'Params1>, 
            paramSpecifier2: ParamSpecifier<'Params2>,
            paramSpecifier3: ParamSpecifier<'Params3>,
            resultSpecifier: ResultSpecifier<'Result>,
            [<CallerFilePath; Optional; DefaultParameterValue("")>] sourcePath: string,
            [<CallerLineNumber; Optional; DefaultParameterValue(0)>] sourceLine: int)
            : 'Params1 -> 'Params2 -> 'Params3 -> DbCall<'Result> = 
        this.Sql((fun _ -> commandText), paramSpecifier1, paramSpecifier2, paramSpecifier3, resultSpecifier, sourcePath, sourceLine)


    /// <summary>
    /// Builds a query function with three curried args based on raw SQL text.
    /// </summary>
    /// <param name="commandText">
    /// The SQL command text.
    /// </param>
    /// <param name="name1">
    /// The first parameter name.
    /// </param>
    /// <param name="name2">
    /// The second parameter name.
    /// </param>
    /// <param name="name3">
    /// The third parameter name.
    /// </param>
    /// <param name="sourcePath">
    /// The calling source path for diagnostic purposes.
    /// </param>
    /// <param name="sourceLine">
    /// The calling source path for diagnostic purposes.
    /// </param>
    /// <param name="resultSpecifier">
    /// The result builder.
    /// </param>
    member this.Sql<'Params1, 'Params2, 'Params3, 'Result> (
            commandText: string,
            name1: string, name2: string, name3: string, 
            resultSpecifier: ResultSpecifier<'Result>,
            [<CallerFilePath; Optional; DefaultParameterValue("")>] sourcePath: string,
            [<CallerLineNumber; Optional; DefaultParameterValue(0)>] sourceLine: int)
            : 'Params1 -> 'Params2 -> 'Params3 -> DbCall<'Result> =         
        this.Sql(commandText, Params.Auto<'Params1>(name1), Params.Auto<'Params2>(name2), Params.Auto<'Params3>(name3), resultSpecifier, sourcePath, sourceLine)

    /// <summary>
    /// Builds a query function with four curried args based on raw SQL text.
    /// </summary>
    /// <param name="template">
    /// The SQL command template.
    /// </param>
    /// <param name="paramSpecifier1">
    /// The first parameter builder.
    /// </param>
    /// <param name="paramSpecifier2">
    /// The second parameter builder.
    /// </param>
    /// <param name="paramSpecifier3">
    /// The third parameter builder.
    /// </param>
    /// <param name="paramSpecifier4">
    /// The fourth parameter builder.
    /// </param>
    /// <param name="sourcePath">
    /// The calling source path for diagnostic purposes.
    /// </param>
    /// <param name="sourceLine">
    /// The calling source path for diagnostic purposes.
    /// </param>
    /// <param name="resultSpecifier">
    /// The result builder.
    /// </param>
    member __.Sql (
            template: ('Params1 * 'Params2 * 'Params3 * 'Params4) option -> string,
            paramSpecifier1: ParamSpecifier<'Params1>, 
            paramSpecifier2: ParamSpecifier<'Params2>,
            paramSpecifier3: ParamSpecifier<'Params3>,
            paramSpecifier4: ParamSpecifier<'Params4>,
            resultSpecifier: ResultSpecifier<'Result>,
            [<CallerFilePath; Optional; DefaultParameterValue("")>] sourcePath: string,
            [<CallerLineNumber; Optional; DefaultParameterValue(0)>] sourceLine: int)
            : 'Params1 -> 'Params2 -> 'Params3 -> 'Params4 -> DbCall<'Result> = 
        try                        
            let provider = GenericSetters.BaseSetterProvider<unit, IDbCommand>(config.ParamBuilders)
            let paramSetter1 = paramSpecifier1(provider, ())
            let paramSetter2 = paramSpecifier2(provider, ())
            let paramSetter3 = paramSpecifier3(provider, ())
            let paramSetter4 = paramSpecifier4(provider, ())

            let rowGetterProvider = GenericGetters.BaseGetterProvider<IDataRecord, IDataRecord>(config.RowBuilders)
            let resultSpecifier' prototype = resultSpecifier(rowGetterProvider, prototype)

            let setArtificial(command: IDbCommand) = 
                paramSetter1.SetArtificial(None, command)
                paramSetter2.SetArtificial(None, command)
                paramSetter3.SetArtificial(None, command)                    
                paramSetter4.SetArtificial(None, command)                    

            let resultReader = executePrototypeQuery(CommandType.Text, template(None), setArtificial, resultSpecifier')

            let setParams (parameters1: 'Params1, parameters2: 'Params2, parameters3: 'Params3, parameters4: 'Params4) (command: IDbCommand) = 
                paramSetter1.SetValue(parameters1, None, command) 
                paramSetter2.SetValue(parameters2, None, command)
                paramSetter3.SetValue(parameters3, None, command)
                paramSetter4.SetValue(parameters4, None, command)

            fun (parameters1: 'Params1) (parameters2: 'Params2) (parameters3: 'Params3) (parameters4: 'Params4) (provider: IConnector) ->
                executeQuery(provider, template(Some (parameters1, parameters2, parameters3, parameters4)), resultReader, setParams(parameters1, parameters2, parameters3, parameters4))
        with ex ->
            handleException(sourcePath, sourceLine, ex)


    /// <summary>
    /// Builds a query function with four curried args based on a command template.
    /// </summary>
    /// <param name="template">
    /// The SQL command template.
    /// </param>
    /// <param name="name1">
    /// The first parameter name.
    /// </param>
    /// <param name="name2">
    /// The second parameter name.
    /// </param>
    /// <param name="name3">
    /// The third parameter name.
    /// </param>
    /// <param name="name4">
    /// The fourth parameter name.
    /// </param>
    /// <param name="sourcePath">
    /// The calling source path for diagnostic purposes.
    /// </param>
    /// <param name="sourceLine">
    /// The calling source path for diagnostic purposes.
    /// </param>
    /// <param name="resultSpecifier">
    /// The result builder.
    /// </param>
    member this.Sql<'Params1, 'Params2, 'Params3, 'Params4, 'Result> (
            template: ('Params1 * 'Params2 * 'Params3 * 'Params4) option -> string,
            name1: string, name2: string, name3: string, name4: string, 
            resultSpecifier: ResultSpecifier<'Result>,
            [<CallerFilePath; Optional; DefaultParameterValue("")>] sourcePath: string,
            [<CallerLineNumber; Optional; DefaultParameterValue(0)>] sourceLine: int)
            : 'Params1 -> 'Params2 -> 'Params3 -> 'Params4 -> DbCall<'Result> =         
        this.Sql(template, Params.Auto<'Params1>(name1), Params.Auto<'Params2>(name2), 
                 Params.Auto<'Params3>(name3), Params.Auto<'Params4>(name4), 
                 resultSpecifier,
                 sourcePath, sourceLine)

    /// <summary>
    /// Builds a query function with four curried args based on a command template.
    /// </summary>
    /// <param name="template">
    /// The SQL command template.
    /// </param>
    /// <param name="argName1">
    /// The first parameter name.
    /// </param>
    /// <param name="argName2">
    /// The second parameter name.
    /// </param>
    /// <param name="argName3">
    /// The third parameter name.
    /// </param>
    /// <param name="argName4">
    /// The fourth parameter name.
    /// </param>
    /// <param name="resultName">
    /// The result column name.
    /// </param>
    /// <param name="sourcePath">
    /// The calling source path for diagnostic purposes.
    /// </param>
    /// <param name="sourceLine">
    /// The calling source path for diagnostic purposes.
    /// </param>
    member this.Sql<'Params1, 'Params2, 'Params3, 'Params4, 'Result> (
            template: ('Params1 * 'Params2 * 'Params3 * 'Params4) option -> string,
            [<Optional>] argName1: string, [<Optional>] argName2: string, [<Optional>] argName3: string, [<Optional>] argName4: string, 
            [<Optional>] resultName: string,
            [<CallerFilePath; Optional; DefaultParameterValue("")>] sourcePath: string,
            [<CallerLineNumber; Optional; DefaultParameterValue(0)>] sourceLine: int)
            : 'Params1 -> 'Params2 -> 'Params3 -> 'Params4 -> DbCall<'Result> =         
        this.Sql(template, 
                 Params.Auto<'Params1>(argName1), Params.Auto<'Params2>(argName2), 
                 Params.Auto<'Params3>(argName3), Params.Auto<'Params4>(argName4), 
                 Results.Auto<'Result>(resultName), 
                 sourcePath, sourceLine) 

    /// <summary>
    /// Builds a query function with four curried args based on raw SQL text.
    /// </summary>
    /// <param name="commandText">
    /// The SQL command text.
    /// </param>
    /// <param name="paramSpecifier1">
    /// The first parameter builder.
    /// </param>
    /// <param name="paramSpecifier2">
    /// The second parameter builder.
    /// </param>
    /// <param name="paramSpecifier3">
    /// The third parameter builder.
    /// </param>
    /// <param name="paramSpecifier4">
    /// The fourth parameter builder.
    /// </param>
    /// <param name="sourcePath">
    /// The calling source path for diagnostic purposes.
    /// </param>
    /// <param name="sourceLine">
    /// The calling source path for diagnostic purposes.
    /// </param>
    /// <param name="resultSpecifier">
    /// The result builder.
    /// </param>
    member this.Sql (
            commandText: string,
            paramSpecifier1: ParamSpecifier<'Params1>, 
            paramSpecifier2: ParamSpecifier<'Params2>,
            paramSpecifier3: ParamSpecifier<'Params3>,
            paramSpecifier4: ParamSpecifier<'Params4>,
            resultSpecifier: ResultSpecifier<'Result>,
            [<CallerFilePath; Optional; DefaultParameterValue("")>] sourcePath: string,
            [<CallerLineNumber; Optional; DefaultParameterValue(0)>] sourceLine: int)
            : 'Params1 -> 'Params2 -> 'Params3 -> 'Params4 -> DbCall<'Result> = 
        this.Sql((fun _ -> commandText), paramSpecifier1, paramSpecifier2, paramSpecifier3, paramSpecifier4, resultSpecifier, sourcePath, sourceLine)

    /// <summary>
    /// Builds a query function with four curried args based on raw SQL text.
    /// </summary>
    /// <param name="commandText">
    /// The SQL command text.
    /// </param>
    /// <param name="name1">
    /// The first parameter name.
    /// </param>
    /// <param name="name2">
    /// The second parameter name.
    /// </param>
    /// <param name="name3">
    /// The third parameter name.
    /// </param>
    /// <param name="name4">
    /// The fourth parameter name.
    /// </param>
    /// <param name="sourcePath">
    /// The calling source path for diagnostic purposes.
    /// </param>
    /// <param name="sourceLine">
    /// The calling source path for diagnostic purposes.
    /// </param>
    /// <param name="resultSpecifier">
    /// The result builder.
    /// </param>
    member this.Sql<'Params1, 'Params2, 'Params3, 'Params4, 'Result> (
            commandText: string,
            name1: string, name2: string, name3: string, name4: string, 
            resultSpecifier: ResultSpecifier<'Result>,
            [<CallerFilePath; Optional; DefaultParameterValue("")>] sourcePath: string,
            [<CallerLineNumber; Optional; DefaultParameterValue(0)>] sourceLine: int)
            : 'Params1 -> 'Params2 -> 'Params3 -> 'Params4 -> DbCall<'Result> =         
        this.Sql(commandText, Params.Auto<'Params1>(name1), Params.Auto<'Params2>(name2), 
                 Params.Auto<'Params3>(name3), Params.Auto<'Params4>(name4), 
                 resultSpecifier,
                 sourcePath, sourceLine)

    /// <summary>
    /// Builds a query function with five curried args based on a command template.
    /// </summary>
    /// <param name="template">
    /// The SQL command template.
    /// </param>
    /// <param name="paramSpecifier1">
    /// The first parameter builder.
    /// </param>
    /// <param name="paramSpecifier2">
    /// The second parameter builder.
    /// </param>
    /// <param name="paramSpecifier3">
    /// The third parameter builder.
    /// </param>
    /// <param name="paramSpecifier4">
    /// The fourth parameter builder.
    /// </param>
    /// <param name="paramSpecifier5">
    /// The fifth parameter builder.
    /// </param>
    /// <param name="sourcePath">
    /// The calling source path for diagnostic purposes.
    /// </param>
    /// <param name="sourceLine">
    /// The calling source path for diagnostic purposes.
    /// </param>
    /// <param name="resultSpecifier">
    /// The result builder.
    /// </param>
    member __.Sql (
            template: ('Params1 * 'Params2 * 'Params3 * 'Params4 * 'Params5) option -> string,
            paramSpecifier1: ParamSpecifier<'Params1>, 
            paramSpecifier2: ParamSpecifier<'Params2>,
            paramSpecifier3: ParamSpecifier<'Params3>,
            paramSpecifier4: ParamSpecifier<'Params4>,
            paramSpecifier5: ParamSpecifier<'Params5>,
            resultSpecifier: ResultSpecifier<'Result>,
            [<CallerFilePath; Optional; DefaultParameterValue("")>] sourcePath: string,
            [<CallerLineNumber; Optional; DefaultParameterValue(0)>] sourceLine: int)
            : 'Params1 -> 'Params2 -> 'Params3 -> 'Params4 -> 'Params5 -> DbCall<'Result> = 
        try                        
            let provider = GenericSetters.BaseSetterProvider<unit, IDbCommand>(config.ParamBuilders)
            let paramSetter1 = paramSpecifier1(provider, ())
            let paramSetter2 = paramSpecifier2(provider, ())
            let paramSetter3 = paramSpecifier3(provider, ())
            let paramSetter4 = paramSpecifier4(provider, ())
            let paramSetter5 = paramSpecifier5(provider, ())

            let rowGetterProvider = GenericGetters.BaseGetterProvider<IDataRecord, IDataRecord>(config.RowBuilders)
            let resultSpecifier' prototype = resultSpecifier(rowGetterProvider, prototype)

            let setArtificial(command: IDbCommand) = 
                paramSetter1.SetArtificial(None, command)
                paramSetter2.SetArtificial(None, command)
                paramSetter3.SetArtificial(None, command)                    
                paramSetter4.SetArtificial(None, command)                    
                paramSetter5.SetArtificial(None, command)                    

            let resultReader = executePrototypeQuery(CommandType.Text, template(None), setArtificial, resultSpecifier')

            let setParams (parameters1: 'Params1, parameters2: 'Params2, parameters3: 'Params3, parameters4: 'Params4, parameters5: 'Params5) (command: IDbCommand) = 
                paramSetter1.SetValue(parameters1, None, command) 
                paramSetter2.SetValue(parameters2, None, command)
                paramSetter3.SetValue(parameters3, None, command)
                paramSetter4.SetValue(parameters4, None, command)
                paramSetter5.SetValue(parameters5, None, command)

            fun (parameters1: 'Params1) (parameters2: 'Params2) (parameters3: 'Params3) (parameters4: 'Params4) (parameters5: 'Params5) (provider: IConnector) ->
                executeQuery(
                    provider, 
                    template(Some (parameters1, parameters2, parameters3, parameters4, parameters5)), 
                    resultReader, 
                    setParams(parameters1, parameters2, parameters3, parameters4, parameters5))
        with ex ->
            handleException(sourcePath, sourceLine, ex)

    /// <summary>
    /// Builds a query function with five curried args based on a command template.
    /// </summary>
    /// <param name="template">
    /// The SQL command template.
    /// </param>
    /// <param name="name1">
    /// The first parameter name.
    /// </param>
    /// <param name="name2">
    /// The second parameter name.
    /// </param>
    /// <param name="name3">
    /// The third parameter name.
    /// </param>
    /// <param name="name4">
    /// The fourth parameter name.
    /// </param>
    /// <param name="name5">
    /// The fifth parameter name.
    /// </param>
    /// <param name="sourcePath">
    /// The calling source path for diagnostic purposes.
    /// </param>
    /// <param name="sourceLine">
    /// The calling source path for diagnostic purposes.
    /// </param>
    /// <param name="resultSpecifier">
    /// The result builder.
    /// </param>
    member this.Sql<'Params1, 'Params2, 'Params3, 'Params4, 'Params5, 'Result> (
            template: ('Params1 * 'Params2 * 'Params3 * 'Params4 * 'Params5) option -> string,
            name1: string, name2: string, name3: string, name4: string, name5: string, 
            resultSpecifier: ResultSpecifier<'Result>,
            [<CallerFilePath; Optional; DefaultParameterValue("")>] sourcePath: string,
            [<CallerLineNumber; Optional; DefaultParameterValue(0)>] sourceLine: int)
            : 'Params1 -> 'Params2 -> 'Params3 -> 'Params4 -> 'Params5 -> DbCall<'Result> =         
        this.Sql(template, 
                 Params.Auto<'Params1>(name1), Params.Auto<'Params2>(name2), 
                 Params.Auto<'Params3>(name3), Params.Auto<'Params4>(name4), 
                 Params.Auto<'Params5>(name5), 
                 resultSpecifier,
                 sourcePath, sourceLine)


    /// <summary>
    /// Builds a query function with four curried args based on a command template.
    /// </summary>
    /// <param name="template">
    /// The SQL command template.
    /// </param>
    /// <param name="argName1">
    /// The first parameter name.
    /// </param>
    /// <param name="argName2">
    /// The second parameter name.
    /// </param>
    /// <param name="argName3">
    /// The third parameter name.
    /// </param>
    /// <param name="argName4">
    /// The fourth parameter name.
    /// </param>
    /// <param name="argName5">
    /// The fifth parameter name.
    /// </param>
    /// <param name="resultName">
    /// The result column name.
    /// </param>
    /// <param name="sourcePath">
    /// The calling source path for diagnostic purposes.
    /// </param>
    /// <param name="sourceLine">
    /// The calling source path for diagnostic purposes.
    /// </param>
    member this.Sql<'Params1, 'Params2, 'Params3, 'Params4, 'Params5, 'Result> (
            template: ('Params1 * 'Params2 * 'Params3 * 'Params4 * 'Params5) option -> string,
            [<Optional>] argName1: string, [<Optional>] argName2: string, [<Optional>] argName3: string, argName4: string, [<Optional>] argName5: string, 
            [<Optional>] resultName: string,
            [<CallerFilePath; Optional; DefaultParameterValue("")>] sourcePath: string,
            [<CallerLineNumber; Optional; DefaultParameterValue(0)>] sourceLine: int)
            : 'Params1 -> 'Params2 -> 'Params3 -> 'Params4 -> 'Params5 -> DbCall<'Result> =         
        this.Sql(template, 
                 Params.Auto<'Params1>(argName1), Params.Auto<'Params2>(argName2), 
                 Params.Auto<'Params3>(argName3), Params.Auto<'Params4>(argName4), 
                 Params.Auto<'Params5>(argName5), 
                 Results.Auto<'Result>(resultName), 
                 sourcePath, sourceLine) 

    /// <summary>
    /// Builds a query function with five curried args based on raw SQL text.
    /// </summary>
    /// <param name="commandText">
    /// The SQL command text.
    /// </param>
    /// <param name="paramSpecifier1">
    /// The first parameter builder.
    /// </param>
    /// <param name="paramSpecifier2">
    /// The second parameter builder.
    /// </param>
    /// <param name="paramSpecifier3">
    /// The third parameter builder.
    /// </param>
    /// <param name="paramSpecifier4">
    /// The fourth parameter builder.
    /// </param>
    /// <param name="paramSpecifier5">
    /// The fifth parameter builder.
    /// </param>
    /// <param name="sourcePath">
    /// The calling source path for diagnostic purposes.
    /// </param>
    /// <param name="sourceLine">
    /// The calling source path for diagnostic purposes.
    /// </param>
    /// <param name="resultSpecifier">
    /// The result builder.
    /// </param>
    member this.Sql (
            commandText: string,
            paramSpecifier1: ParamSpecifier<'Params1>, 
            paramSpecifier2: ParamSpecifier<'Params2>,
            paramSpecifier3: ParamSpecifier<'Params3>,
            paramSpecifier4: ParamSpecifier<'Params4>,
            paramSpecifier5: ParamSpecifier<'Params5>,
            resultSpecifier: ResultSpecifier<'Result>,
            [<CallerFilePath; Optional; DefaultParameterValue("")>] sourcePath: string,
            [<CallerLineNumber; Optional; DefaultParameterValue(0)>] sourceLine: int)
            : 'Params1 -> 'Params2 -> 'Params3 -> 'Params4 -> 'Params5 -> DbCall<'Result> = 
        this.Sql((fun _ -> commandText), paramSpecifier1, paramSpecifier2, paramSpecifier3, paramSpecifier4, paramSpecifier5, resultSpecifier, sourcePath, sourceLine)

    /// <summary>
    /// Builds a query function with five curried args based on raw SQL text.
    /// </summary>
    /// <param name="commandText">
    /// The SQL command text.
    /// </param>
    /// <param name="name1">
    /// The first parameter name.
    /// </param>
    /// <param name="name2">
    /// The second parameter name.
    /// </param>
    /// <param name="name3">
    /// The third parameter name.
    /// </param>
    /// <param name="name4">
    /// The fourth parameter name.
    /// </param>
    /// <param name="name5">
    /// The fifth parameter name.
    /// </param>
    /// <param name="sourcePath">
    /// The calling source path for diagnostic purposes.
    /// </param>
    /// <param name="sourceLine">
    /// The calling source path for diagnostic purposes.
    /// </param>
    /// <param name="resultSpecifier">
    /// The result builder.
    /// </param>
    member this.Sql<'Params1, 'Params2, 'Params3, 'Params4, 'Params5, 'Result> (
            commandText: string,
            name1: string, name2: string, name3: string, name4: string, name5: string, 
            resultSpecifier: ResultSpecifier<'Result>,
            [<CallerFilePath; Optional; DefaultParameterValue("")>] sourcePath: string,
            [<CallerLineNumber; Optional; DefaultParameterValue(0)>] sourceLine: int)
            : 'Params1 -> 'Params2 -> 'Params3 -> 'Params4 -> 'Params5 -> DbCall<'Result> =         
        this.Sql(commandText, 
                 Params.Auto<'Params1>(name1), Params.Auto<'Params2>(name2), 
                 Params.Auto<'Params3>(name3), Params.Auto<'Params4>(name4), 
                 Params.Auto<'Params5>(name5), 
                 resultSpecifier,
                 sourcePath, sourceLine)

    /// <summary>
    /// Builds a query function with two curried args based on raw SQL text.
    /// </summary>
    /// <param name="commandText">
    /// The SQL command text.
    /// </param>
    /// <param name="argName1">
    /// The first parameter name.
    /// </param>
    /// <param name="argName2">
    /// The second parameter name.
    /// </param>
    /// <param name="resultName">
    /// The result column name.
    /// </param>
    /// <param name="sourcePath">
    /// The calling source path for diagnostic purposes.
    /// </param>
    /// <param name="sourceLine">
    /// The calling source path for diagnostic purposes.
    /// </param>
    member this.Sql<'Params1, 'Params2, 'Result> (
            commandText: string,
            [<Optional>] argName1: string, [<Optional>] argName2: string, [<Optional>] resultName: string,
            [<CallerFilePath; Optional; DefaultParameterValue("")>] sourcePath: string,
            [<CallerLineNumber; Optional; DefaultParameterValue(0)>] sourceLine: int)
            : 'Params1 -> 'Params2 -> DbCall<'Result> =         
        this.Sql(commandText, Params.Auto<'Params1>(argName1), Params.Auto<'Params2>(argName2), Results.Auto<'Result>(resultName), sourcePath, sourceLine) 

    /// <summary>
    /// Builds a query function with three curried args based on raw SQL text.
    /// </summary>
    /// <param name="commandText">
    /// The SQL command text.
    /// </param>
    /// <param name="argName1">
    /// The first parameter name.
    /// </param>
    /// <param name="argName2">
    /// The second parameter name.
    /// </param>
    /// <param name="argName3">
    /// The third parameter name.
    /// </param>
    /// <param name="resultName">
    /// The result column name.
    /// </param>
    /// <param name="sourcePath">
    /// The calling source path for diagnostic purposes.
    /// </param>
    /// <param name="sourceLine">
    /// The calling source path for diagnostic purposes.
    /// </param>
    member this.Sql<'Params1, 'Params2, 'Params3, 'Result> (
            commandText: string,
            [<Optional>] argName1: string, [<Optional>] argName2: string, [<Optional>] argName3: string, [<Optional>] resultName: string,
            [<CallerFilePath; Optional; DefaultParameterValue("")>] sourcePath: string,
            [<CallerLineNumber; Optional; DefaultParameterValue(0)>] sourceLine: int)
            : 'Params1 -> 'Params2 -> 'Params3 -> DbCall<'Result> =         
        this.Sql(commandText, Params.Auto<'Params1>(argName1), Params.Auto<'Params2>(argName2), Params.Auto<'Params3>(argName3), Results.Auto<'Result>(resultName), sourcePath, sourceLine) 

    /// <summary>
    /// Builds a query function with four curried args based on raw SQL text.
    /// </summary>
    /// <param name="commandText">
    /// The SQL command text.
    /// </param>
    /// <param name="argName1">
    /// The first parameter name.
    /// </param>
    /// <param name="argName2">
    /// The second parameter name.
    /// </param>
    /// <param name="argName3">
    /// The third parameter name.
    /// </param>
    /// <param name="argName4">
    /// The fourth parameter name.
    /// </param>
    /// <param name="resultName">
    /// The result column name.
    /// </param>
    /// <param name="sourcePath">
    /// The calling source path for diagnostic purposes.
    /// </param>
    /// <param name="sourceLine">
    /// The calling source path for diagnostic purposes.
    /// </param>
    member this.Sql<'Params1, 'Params2, 'Params3, 'Params4, 'Result> (
            commandText: string,
            [<Optional>] argName1: string, [<Optional>] argName2: string, [<Optional>] argName3: string, [<Optional>] argName4: string, 
            [<Optional>] resultName: string,
            [<CallerFilePath; Optional; DefaultParameterValue("")>] sourcePath: string,
            [<CallerLineNumber; Optional; DefaultParameterValue(0)>] sourceLine: int)
            : 'Params1 -> 'Params2 -> 'Params3 -> 'Params4 -> DbCall<'Result> =         
        this.Sql(commandText, 
                 Params.Auto<'Params1>(argName1), Params.Auto<'Params2>(argName2), 
                 Params.Auto<'Params3>(argName3), Params.Auto<'Params4>(argName4), 
                 Results.Auto<'Result>(resultName), 
                 sourcePath, sourceLine) 

    /// <summary>
    /// Builds a query function with four curried args based on raw SQL text.
    /// </summary>
    /// <param name="commandText">
    /// The SQL command text.
    /// </param>
    /// <param name="argName1">
    /// The first parameter name.
    /// </param>
    /// <param name="argName2">
    /// The second parameter name.
    /// </param>
    /// <param name="argName3">
    /// The third parameter name.
    /// </param>
    /// <param name="argName4">
    /// The fourth parameter name.
    /// </param>
    /// <param name="argName5">
    /// The fifth parameter name.
    /// </param>
    /// <param name="resultName">
    /// The result column name.
    /// </param>
    /// <param name="sourcePath">
    /// The calling source path for diagnostic purposes.
    /// </param>
    /// <param name="sourceLine">
    /// The calling source path for diagnostic purposes.
    /// </param>
    member this.Sql<'Params1, 'Params2, 'Params3, 'Params4, 'Params5, 'Result> (
            commandText: string,
            [<Optional>] argName1: string, [<Optional>] argName2: string, [<Optional>] argName3: string, argName4: string, [<Optional>] argName5: string, 
            [<Optional>] resultName: string,
            [<CallerFilePath; Optional; DefaultParameterValue("")>] sourcePath: string,
            [<CallerLineNumber; Optional; DefaultParameterValue(0)>] sourceLine: int)
            : 'Params1 -> 'Params2 -> 'Params3 -> 'Params4 -> 'Params5 -> DbCall<'Result> =         
        this.Sql(commandText, 
                 Params.Auto<'Params1>(argName1), Params.Auto<'Params2>(argName2), 
                 Params.Auto<'Params3>(argName3), Params.Auto<'Params4>(argName4), 
                 Params.Auto<'Params5>(argName5), 
                 Results.Auto<'Result>(resultName), 
                 sourcePath, sourceLine) 

    /// <summary>
    /// Builds a one arg query function invoking stored procedure.
    /// </summary>
    /// <param name="procName">
    /// The stored procedure name.
    /// </param>
    /// <param name="paramSpecifier">
    /// The parameter builder.
    /// </param>
    /// <param name="sourcePath">
    /// The calling source path for diagnostic purposes.
    /// </param>
    /// <param name="sourceLine">
    /// The calling source path for diagnostic purposes.
    /// </param>
    /// <param name="outParamSpecifier">
    /// The output parameter builder.
    /// </param>
    /// <param name="resultSpecifier">
    /// The result builder.
    /// </param>
    member __.Proc (procName: string, 
                    paramSpecifier: ParamSpecifier<'Params>,
                    outParamSpecifier: OutParamSpecifier<'OutParams>, 
                    resultSpecifier: ResultSpecifier<'Result>,
                    [<CallerFilePath; Optional; DefaultParameterValue("")>] sourcePath: string,
                    [<CallerLineNumber; Optional; DefaultParameterValue(0)>] sourceLine: int)
                    : 'Params -> DbCall<'Result * 'OutParams> = 
            try                        
                let provider = GenericSetters.BaseSetterProvider<unit, IDbCommand>(config.ParamBuilders)
                let paramSetter = paramSpecifier(provider, ())
                        
                let outParamProvider = GenericGetters.BaseGetterProvider<unit, IDbCommand>(config.OutParamBuilders)
                let outParamGetter = outParamSpecifier(outParamProvider, ())

                let setArtificial(command: IDbCommand) = 
                    paramSetter.SetArtificial(None, command)
                    outParamGetter.Create(command)

                let rowGetterProvider = GenericGetters.BaseGetterProvider<IDataRecord, IDataRecord>(config.RowBuilders)
                let resultSpecifier' prototype = resultSpecifier(rowGetterProvider, prototype)

                let resultReader = executePrototypeQuery(CommandType.StoredProcedure, procName, setArtificial, resultSpecifier')
                fun (parameters: 'Params) (provider: IConnector) ->
                    executeProcedure(provider, procName, outParamGetter, resultReader, fun cmd -> paramSetter.SetValue(parameters, None, cmd))
            with ex ->
                handleException(sourcePath, sourceLine, ex)

    /// <summary>
    /// Builds a one arg query function invoking stored procedure.
    /// </summary>
    /// <param name="procName">
    /// The stored procedure name.
    /// </param>
    /// <param name="name">
    /// The parameter name.
    /// </param>
    /// <param name="sourcePath">
    /// The calling source path for diagnostic purposes.
    /// </param>
    /// <param name="sourceLine">
    /// The calling source path for diagnostic purposes.
    /// </param>
    /// <param name="outParamSpecifier">
    /// The output parameter builder.
    /// </param>
    /// <param name="resultSpecifier">
    /// The result builder.
    /// </param>
    member this.Proc<'Params, 'OutParams, 'Result> (
            procName: string,    
            name: string,
            outParamSpecifier: OutParamSpecifier<'OutParams>, 
            resultSpecifier: ResultSpecifier<'Result>,
            [<CallerFilePath; Optional; DefaultParameterValue("")>] sourcePath: string,
            [<CallerLineNumber; Optional; DefaultParameterValue(0)>] sourceLine: int)
            : 'Params -> DbCall<'Result * 'OutParams> = 
        this.Proc(procName, Params.Auto<'Params>(name), outParamSpecifier, resultSpecifier, sourcePath, sourceLine)


    /// <summary>
    /// Builds a one arg query function invoking stored procedure.
    /// </summary>
    /// <param name="procName">
    /// The stored procedure name.
    /// </param>
    /// <param name="name">
    /// The parameter name.
    /// </param>
    /// <param name="sourcePath">
    /// The calling source path for diagnostic purposes.
    /// </param>
    /// <param name="sourceLine">
    /// The calling source path for diagnostic purposes.
    /// </param>
    /// <param name="outParamName">
    /// The output parameter name.
    /// </param>
    /// <param name="resultName">
    /// The result column name.
    /// </param>
    member this.Proc<'Params, 'OutParams, 'Result> (
            procName: string,
            [<Optional>] name: string,
            [<Optional>] outParamName: string, 
            [<Optional>] resultName: string,
            [<CallerFilePath; Optional; DefaultParameterValue("")>] sourcePath: string,
            [<CallerLineNumber; Optional; DefaultParameterValue(0)>] sourceLine: int)
            : 'Params -> DbCall<'Result * 'OutParams> = 
        this.Proc(procName, Params.Auto<'Params>(name), OutParams.Auto<'OutParams>(outParamName), Results.Auto<'Result>(resultName), sourcePath, sourceLine)

    /// <summary>
    /// Builds a query function with two curried args invoking stored procedure.
    /// </summary>
    /// <param name="procName">
    /// The stored procedure name.
    /// </param>
    /// <param name="paramSpecifier1">
    /// The first parameter builder.
    /// </param>
    /// <param name="paramSpecifier2">
    /// The second parameter builder.
    /// </param>
    /// <param name="sourcePath">
    /// The calling source path for diagnostic purposes.
    /// </param>
    /// <param name="sourceLine">
    /// The calling source path for diagnostic purposes.
    /// </param>
    /// <param name="outParamSpecifier">
    /// The output parameter builder.
    /// </param>
    /// <param name="resultSpecifier">
    /// The result builder.
    /// </param>
    member __.Proc (procName: string, 
                    paramSpecifier1: ParamSpecifier<'Params1>,
                    paramSpecifier2: ParamSpecifier<'Params2>,
                    outParamSpecifier: OutParamSpecifier<'OutParams>, 
                    resultSpecifier: ResultSpecifier<'Result>,
                    [<CallerFilePath; Optional; DefaultParameterValue("")>] sourcePath: string,
                    [<CallerLineNumber; Optional; DefaultParameterValue(0)>] sourceLine: int)
                    : 'Params1 -> 'Params2 -> DbCall<'Result * 'OutParams> = 
        try                        
            let provider = GenericSetters.BaseSetterProvider<unit, IDbCommand>(config.ParamBuilders)
            let paramSetter1 = paramSpecifier1(provider, ())
            let paramSetter2 = paramSpecifier2(provider, ())
                        
            let outParamProvider = GenericGetters.BaseGetterProvider<unit, IDbCommand>(config.OutParamBuilders)
            let outParamGetter = outParamSpecifier(outParamProvider, ())

            let setArtificial(command: IDbCommand) = 
                paramSetter1.SetArtificial(None, command)
                paramSetter2.SetArtificial(None, command)
                outParamGetter.Create(command)

            let rowGetterProvider = GenericGetters.BaseGetterProvider<IDataRecord, IDataRecord>(config.RowBuilders)
            let resultSpecifier' prototype = resultSpecifier(rowGetterProvider, prototype)

            let resultReader = executePrototypeQuery(CommandType.StoredProcedure, procName, setArtificial, resultSpecifier')

            let setParams (parameters1: 'Params1, parameters2: 'Params2) (command: IDbCommand) = 
                paramSetter1.SetValue(parameters1, None, command)
                paramSetter2.SetValue(parameters2, None, command)

            fun (parameters1: 'Params1) (parameters2: 'Params2) (provider: IConnector) ->
                executeProcedure(provider, procName, outParamGetter, resultReader, setParams(parameters1, parameters2))
        with ex ->
            handleException(sourcePath, sourceLine, ex)

    /// <summary>
    /// Builds a query function with two curried args invoking stored procedure.
    /// </summary>
    /// <param name="procName">
    /// The stored procedure name.
    /// </param>
    /// <param name="name1">
    /// The first parameter name.
    /// </param>
    /// <param name="name2">
    /// The second parameter name.
    /// </param>
    /// <param name="sourcePath">
    /// The calling source path for diagnostic purposes.
    /// </param>
    /// <param name="sourceLine">
    /// The calling source path for diagnostic purposes.
    /// </param>
    /// <param name="outParamName">
    /// The output parameter name.
    /// </param>
    /// <param name="resultName">
    /// The result column name.
    /// </param>
    member this.Proc<'Params1, 'Params2, 'OutParams, 'Result> (
            procName: string,
            [<Optional>] name1: string, [<Optional>] name2: string,
            [<Optional>] outParamName: string, 
            [<Optional>] resultName: string,
            [<CallerFilePath; Optional; DefaultParameterValue("")>] sourcePath: string,
            [<CallerLineNumber; Optional; DefaultParameterValue(0)>] sourceLine: int)
            : 'Params1 -> 'Params2 -> DbCall<'Result * 'OutParams> = 
        this.Proc(procName, Params.Auto<'Params1>(name1), Params.Auto<'Params2>(name2), OutParams.Auto<'OutParams>(outParamName), Results.Auto<'Result>(resultName), sourcePath, sourceLine)

    /// <summary>
    /// Builds a query function with two curried args invoking stored procedure.
    /// </summary>
    /// <param name="procName">
    /// The stored procedure name.
    /// </param>
    /// <param name="name1">
    /// The first parameter name.
    /// </param>
    /// <param name="name2">
    /// The second parameter name.
    /// </param>
    /// <param name="sourcePath">
    /// The calling source path for diagnostic purposes.
    /// </param>
    /// <param name="sourceLine">
    /// The calling source path for diagnostic purposes.
    /// </param>
    /// <param name="outParamSpecifier">
    /// The output parameter builder.
    /// </param>
    /// <param name="resultSpecifier">
    /// The result builder.
    /// </param>
    member this.Proc<'Params1, 'Params2, 'OutParams, 'Result> (
            procName: string,
            name1: string, name2: string,
            outParamSpecifier: OutParamSpecifier<'OutParams>, 
            resultSpecifier: ResultSpecifier<'Result>,
            [<CallerFilePath; Optional; DefaultParameterValue("")>] sourcePath: string,
            [<CallerLineNumber; Optional; DefaultParameterValue(0)>] sourceLine: int)
            : 'Params1 -> 'Params2 -> DbCall<'Result * 'OutParams> = 
        this.Proc(procName, Params.Auto<'Params1>(name1), Params.Auto<'Params2>(name2), outParamSpecifier, resultSpecifier, sourcePath, sourceLine)


    /// <summary>
    /// Builds a query function with three curried args invoking stored procedure.
    /// </summary>
    /// <param name="procName">
    /// The stored procedure name.
    /// </param>
    /// <param name="paramSpecifier1">
    /// The first parameter builder.
    /// </param>
    /// <param name="paramSpecifier2">
    /// The second parameter builder.
    /// </param>
    /// <param name="paramSpecifier3">
    /// The third parameter builder.
    /// </param>
    /// <param name="sourcePath">
    /// The calling source path for diagnostic purposes.
    /// </param>
    /// <param name="sourceLine">
    /// The calling source path for diagnostic purposes.
    /// </param>
    /// <param name="outParamSpecifier">
    /// The output parameter builder.
    /// </param>
    /// <param name="resultSpecifier">
    /// The result builder.
    /// </param>
    member __.Proc (procName: string,
                    paramSpecifier1: ParamSpecifier<'Params1>,
                    paramSpecifier2: ParamSpecifier<'Params2>,
                    paramSpecifier3: ParamSpecifier<'Params3>,
                    outParamSpecifier: OutParamSpecifier<'OutParams>, 
                    resultSpecifier: ResultSpecifier<'Result>,
                    [<CallerFilePath; Optional; DefaultParameterValue("")>] sourcePath: string,
                    [<CallerLineNumber; Optional; DefaultParameterValue(0)>] sourceLine: int)
                    : 'Params1 -> 'Params2 -> 'Params3 -> DbCall<'Result * 'OutParams> = 
        try                        
            let provider = GenericSetters.BaseSetterProvider<unit, IDbCommand>(config.ParamBuilders)
            let paramSetter1 = paramSpecifier1(provider, ())
            let paramSetter2 = paramSpecifier2(provider, ())
            let paramSetter3 = paramSpecifier3(provider, ())
                        
            let outParamProvider = GenericGetters.BaseGetterProvider<unit, IDbCommand>(config.OutParamBuilders)
            let outParamGetter = outParamSpecifier(outParamProvider, ())

            let setArtificial(command: IDbCommand) = 
                paramSetter1.SetArtificial(None, command)
                paramSetter2.SetArtificial(None, command)
                paramSetter3.SetArtificial(None, command)
                outParamGetter.Create(command)

            let rowGetterProvider = GenericGetters.BaseGetterProvider<IDataRecord, IDataRecord>(config.RowBuilders)
            let resultSpecifier' prototype = resultSpecifier(rowGetterProvider, prototype)

            let resultReader = executePrototypeQuery(CommandType.StoredProcedure, procName, setArtificial, resultSpecifier')

            let setParams (parameters1: 'Params1, parameters2: 'Params2, parameters3: 'Params3) (command: IDbCommand) = 
                paramSetter1.SetValue(parameters1, None, command)
                paramSetter2.SetValue(parameters2, None, command)
                paramSetter3.SetValue(parameters3, None, command)

            fun (parameters1: 'Params1) (parameters2: 'Params2) (parameters3: 'Params3) (provider: IConnector) ->
                executeProcedure(provider, procName, outParamGetter, resultReader, setParams(parameters1, parameters2, parameters3))
        with ex ->
            handleException(sourcePath, sourceLine, ex)

    /// <summary>
    /// Builds a query function with three curried args invoking stored procedure.
    /// </summary>
    /// <param name="procName">
    /// The stored procedure name.
    /// </param>
    /// <param name="name1">
    /// The first parameter name.
    /// </param>
    /// <param name="name2">
    /// The second parameter name.
    /// </param>
    /// <param name="name3">
    /// The third parameter name.
    /// </param>
    /// <param name="sourcePath">
    /// The calling source path for diagnostic purposes.
    /// </param>
    /// <param name="sourceLine">
    /// The calling source path for diagnostic purposes.
    /// </param>
    /// <param name="outParamSpecifier">
    /// The output parameter builder.
    /// </param>
    /// <param name="resultSpecifier">
    /// The result builder.
    /// </param>
    member this.Proc<'Params1, 'Params2, 'Params3, 'OutParams, 'Result> (
            procName: string,
            name1: string, name2: string, name3: string,
            outParamSpecifier: OutParamSpecifier<'OutParams>, 
            resultSpecifier: ResultSpecifier<'Result>,
            [<CallerFilePath; Optional; DefaultParameterValue("")>] sourcePath: string,
            [<CallerLineNumber; Optional; DefaultParameterValue(0)>] sourceLine: int)
            : 'Params1 -> 'Params2 -> 'Params3 -> DbCall<'Result * 'OutParams> = 
        this.Proc(procName, Params.Auto<'Params1>(name1), Params.Auto<'Params2>(name2), Params.Auto<'Params3>(name3), outParamSpecifier, resultSpecifier, sourcePath, sourceLine)

    /// <summary>
    /// Builds a query function with two curried args invoking stored procedure.
    /// </summary>
    /// <param name="procName">
    /// The stored procedure name.
    /// </param>
    /// <param name="name1">
    /// The first parameter name.
    /// </param>
    /// <param name="name2">
    /// The second parameter name.
    /// </param>
    /// <param name="name3">
    /// The second parameter name.
    /// </param>
    /// <param name="sourcePath">
    /// The calling source path for diagnostic purposes.
    /// </param>
    /// <param name="sourceLine">
    /// The calling source path for diagnostic purposes.
    /// </param>
    /// <param name="outParamName">
    /// The output parameter name.
    /// </param>
    /// <param name="resultName">
    /// The result column name.
    /// </param>
    member this.Proc<'Params1, 'Params2, 'Params3, 'OutParams, 'Result> (
                procName: string,
                [<Optional>] name1: string, [<Optional>] name2: string, [<Optional>] name3: string,
                [<Optional>] outParamName: string, 
                [<Optional>] resultName: string,
                [<CallerFilePath; Optional; DefaultParameterValue("")>] sourcePath: string,
                [<CallerLineNumber; Optional; DefaultParameterValue(0)>] sourceLine: int)
                : 'Params1 -> 'Params2 -> 'Params3 -> DbCall<'Result * 'OutParams> = 
        this.Proc(procName, 
                  Params.Auto<'Params1>(name1), Params.Auto<'Params2>(name2), Params.Auto<'Params3>(name3), 
                  OutParams.Auto<'OutParams>(outParamName), Results.Auto<'Result>(resultName), 
                  sourcePath, sourceLine)                

    /// <summary>
    /// Builds a query function with four curried args invoking stored procedure.
    /// </summary>
    /// <param name="procName">
    /// The stored procedure name.
    /// </param>
    /// <param name="paramSpecifier1">
    /// The first parameter builder.
    /// </param>
    /// <param name="paramSpecifier2">
    /// The second parameter builder.
    /// </param>
    /// <param name="paramSpecifier3">
    /// The third parameter builder.
    /// </param>
    /// <param name="paramSpecifier4">
    /// The fourth parameter builder.
    /// </param>
    /// <param name="sourcePath">
    /// The calling source path for diagnostic purposes.
    /// </param>
    /// <param name="sourceLine">
    /// The calling source path for diagnostic purposes.
    /// </param>
    /// <param name="outParamSpecifier">
    /// The output parameter builder.
    /// </param>
    /// <param name="resultSpecifier">
    /// The result builder.
    /// </param>
    member __.Proc (procName: string,
                    paramSpecifier1: ParamSpecifier<'Params1>,
                    paramSpecifier2: ParamSpecifier<'Params2>,
                    paramSpecifier3: ParamSpecifier<'Params3>,
                    paramSpecifier4: ParamSpecifier<'Params4>,
                    outParamSpecifier: OutParamSpecifier<'OutParams>, 
                    resultSpecifier: ResultSpecifier<'Result>,
                    [<CallerFilePath; Optional; DefaultParameterValue("")>] sourcePath: string,
                    [<CallerLineNumber; Optional; DefaultParameterValue(0)>] sourceLine: int)
                    : 'Params1 -> 'Params2 -> 'Params3 -> 'Params4 -> DbCall<'Result * 'OutParams> = 
        try                        
            let provider = GenericSetters.BaseSetterProvider<unit, IDbCommand>(config.ParamBuilders)
            let paramSetter1 = paramSpecifier1(provider, ())
            let paramSetter2 = paramSpecifier2(provider, ())
            let paramSetter3 = paramSpecifier3(provider, ())
            let paramSetter4 = paramSpecifier4(provider, ())
                        
            let outParamProvider = GenericGetters.BaseGetterProvider<unit, IDbCommand>(config.OutParamBuilders)
            let outParamGetter = outParamSpecifier(outParamProvider, ())

            let setArtificial(command: IDbCommand) = 
                paramSetter1.SetArtificial(None, command)
                paramSetter2.SetArtificial(None, command)
                paramSetter3.SetArtificial(None, command)
                paramSetter4.SetArtificial(None, command)
                outParamGetter.Create(command)

            let rowGetterProvider = GenericGetters.BaseGetterProvider<IDataRecord, IDataRecord>(config.RowBuilders)
            let resultSpecifier' prototype = resultSpecifier(rowGetterProvider, prototype)

            let resultReader = executePrototypeQuery(CommandType.StoredProcedure, procName, setArtificial, resultSpecifier')

            let setParams (parameters1: 'Params1, parameters2: 'Params2, parameters3: 'Params3, parameters4: 'Params4) (command: IDbCommand) = 
                paramSetter1.SetValue(parameters1, None, command)
                paramSetter2.SetValue(parameters2, None, command)
                paramSetter3.SetValue(parameters3, None, command)
                paramSetter4.SetValue(parameters4, None, command)

            fun (parameters1: 'Params1) (parameters2: 'Params2) (parameters3: 'Params3) (parameters4: 'Params4) (provider: IConnector) ->
                executeProcedure(provider, procName, outParamGetter, resultReader, setParams(parameters1, parameters2, parameters3, parameters4))
        with ex ->
            handleException(sourcePath, sourceLine, ex)

    /// <summary>
    /// Builds a query function with four curried args invoking stored procedure.
    /// </summary>
    /// <param name="procName">
    /// The stored procedure name.
    /// </param>
    /// <param name="name1">
    /// The first parameter name.
    /// </param>
    /// <param name="name2">
    /// The second parameter name.
    /// </param>
    /// <param name="name3">
    /// The third parameter name.
    /// </param>
    /// <param name="name4">
    /// The fourth parameter name.
    /// </param>
    /// <param name="sourcePath">
    /// The calling source path for diagnostic purposes.
    /// </param>
    /// <param name="sourceLine">
    /// The calling source path for diagnostic purposes.
    /// </param>
    /// <param name="outParamSpecifier">
    /// The output parameter builder.
    /// </param>
    /// <param name="resultSpecifier">
    /// The result builder.
    /// </param>
    member this.Proc<'Params1, 'Params2, 'Params3, 'Params4, 'OutParams, 'Result> (
                procName: string,
                name1: string, name2: string, name3: string, name4: string,
                outParamSpecifier: OutParamSpecifier<'OutParams>, 
                resultSpecifier: ResultSpecifier<'Result>,
                [<CallerFilePath; Optional; DefaultParameterValue("")>] sourcePath: string,
                [<CallerLineNumber; Optional; DefaultParameterValue(0)>] sourceLine: int)
                : 'Params1 -> 'Params2 -> 'Params3 -> 'Params4 -> DbCall<'Result * 'OutParams> = 
        this.Proc(procName, 
                  Params.Auto<'Params1>(name1), Params.Auto<'Params2>(name2), 
                  Params.Auto<'Params3>(name3), Params.Auto<'Params4>(name4), 
                  outParamSpecifier, resultSpecifier,
                  sourcePath, sourceLine)
    /// <summary>
    /// Builds a query function with four curried args invoking stored procedure.
    /// </summary>
    /// <param name="procName">
    /// The stored procedure name.
    /// </param>
    /// <param name="name1">
    /// The first parameter name.
    /// </param>
    /// <param name="name2">
    /// The second parameter name.
    /// </param>
    /// <param name="name3">
    /// The third parameter name.
    /// </param>
    /// <param name="name4">
    /// The fourth parameter name.
    /// </param>
    /// <param name="sourcePath">
    /// The calling source path for diagnostic purposes.
    /// </param>
    /// <param name="sourceLine">
    /// The calling source path for diagnostic purposes.
    /// </param>
    /// <param name="outParamName">
    /// The output parameter name.
    /// </param>
    /// <param name="resultName">
    /// The result column name.
    /// </param>
    member this.Proc<'Params1, 'Params2, 'Params3, 'Params4, 'OutParams, 'Result> (
            procName: string,
            [<Optional>] name1: string, [<Optional>] name2: string, [<Optional>] name3: string, [<Optional>] name4: string,
            [<Optional>] outParamName: string, 
            [<Optional>] resultName: string,
            [<CallerFilePath; Optional; DefaultParameterValue("")>] sourcePath: string,
            [<CallerLineNumber; Optional; DefaultParameterValue(0)>] sourceLine: int)
            : 'Params1 -> 'Params2 -> 'Params3 -> 'Params4 -> DbCall<'Result * 'OutParams> = 
        this.Proc(procName,
                  Params.Auto<'Params1>(name1), Params.Auto<'Params2>(name2), 
                  Params.Auto<'Params3>(name3), Params.Auto<'Params4>(name4), 
                  OutParams.Auto<'OutParams>(outParamName), 
                  Results.Auto<'Result>(resultName), 
                  sourcePath, sourceLine)
                
    /// <summary>
    /// Builds a query function with five curried args invoking stored procedure.
    /// </summary>
    /// <param name="procName">
    /// The stored procedure name.
    /// </param>
    /// <param name="paramSpecifier1">
    /// The first parameter builder.
    /// </param>
    /// <param name="paramSpecifier2">
    /// The second parameter builder.
    /// </param>
    /// <param name="paramSpecifier3">
    /// The third parameter builder.
    /// </param>
    /// <param name="paramSpecifier4">
    /// The fourth parameter builder.
    /// </param>
    /// <param name="paramSpecifier5">
    /// The fifth parameter builder.
    /// </param>
    /// <param name="sourcePath">
    /// The calling source path for diagnostic purposes.
    /// </param>
    /// <param name="sourceLine">
    /// The calling source path for diagnostic purposes.
    /// </param>
    /// <param name="outParamSpecifier">
    /// The output parameter builder.
    /// </param>
    /// <param name="resultSpecifier">
    /// The result builder.
    /// </param>
    member __.Proc (procName: string,
                    paramSpecifier1: ParamSpecifier<'Params1>,
                    paramSpecifier2: ParamSpecifier<'Params2>,
                    paramSpecifier3: ParamSpecifier<'Params3>,
                    paramSpecifier4: ParamSpecifier<'Params4>,
                    paramSpecifier5: ParamSpecifier<'Params5>,
                    outParamSpecifier: OutParamSpecifier<'OutParams>, 
                    resultSpecifier: ResultSpecifier<'Result>,
                    [<CallerFilePath; Optional; DefaultParameterValue("")>] sourcePath: string,
                    [<CallerLineNumber; Optional; DefaultParameterValue(0)>] sourceLine: int)
                    : 'Params1 -> 'Params2 -> 'Params3 -> 'Params4 -> 'Params5 -> DbCall<'Result * 'OutParams> = 
        try                        
            let provider = GenericSetters.BaseSetterProvider<unit, IDbCommand>(config.ParamBuilders)
            let paramSetter1 = paramSpecifier1(provider, ())
            let paramSetter2 = paramSpecifier2(provider, ())
            let paramSetter3 = paramSpecifier3(provider, ())
            let paramSetter4 = paramSpecifier4(provider, ())
            let paramSetter5 = paramSpecifier5(provider, ())
                        
            let outParamProvider = GenericGetters.BaseGetterProvider<unit, IDbCommand>(config.OutParamBuilders)
            let outParamGetter = outParamSpecifier(outParamProvider, ())

            let setArtificial(command: IDbCommand) = 
                paramSetter1.SetArtificial(None, command)
                paramSetter2.SetArtificial(None, command)
                paramSetter3.SetArtificial(None, command)
                paramSetter4.SetArtificial(None, command)
                paramSetter5.SetArtificial(None, command)
                outParamGetter.Create(command)

            let rowGetterProvider = GenericGetters.BaseGetterProvider<IDataRecord, IDataRecord>(config.RowBuilders)
            let resultSpecifier' prototype = resultSpecifier(rowGetterProvider, prototype)

            let resultReader = executePrototypeQuery(CommandType.StoredProcedure, procName, setArtificial, resultSpecifier')

            let setParams (parameters1: 'Params1, parameters2: 'Params2, parameters3: 'Params3, parameters4: 'Params4, parameters5: 'Params5) (command: IDbCommand) = 
                paramSetter1.SetValue(parameters1, None, command)
                paramSetter2.SetValue(parameters2, None, command)
                paramSetter3.SetValue(parameters3, None, command)
                paramSetter4.SetValue(parameters4, None, command)
                paramSetter5.SetValue(parameters5, None, command)

            fun (parameters1: 'Params1) (parameters2: 'Params2) (parameters3: 'Params3) (parameters4: 'Params4) (parameters5: 'Params5) (provider: IConnector) ->
                executeProcedure(provider, procName, outParamGetter, resultReader, setParams(parameters1, parameters2, parameters3, parameters4, parameters5))
        with ex ->
            handleException(sourcePath, sourceLine, ex)

    /// <summary>
    /// Builds a query function with five curried args invoking stored procedure.
    /// </summary>
    /// <param name="procName">
    /// The stored procedure name.
    /// </param>
    /// <param name="name1">
    /// The first parameter name.
    /// </param>
    /// <param name="name2">
    /// The second parameter name.
    /// </param>
    /// <param name="name3">
    /// The third parameter name.
    /// </param>
    /// <param name="name4">
    /// The fourth parameter name.
    /// </param>
    /// <param name="name5">
    /// The fifth parameter name.
    /// </param>
    /// <param name="sourcePath">
    /// The calling source path for diagnostic purposes.
    /// </param>
    /// <param name="sourceLine">
    /// The calling source path for diagnostic purposes.
    /// </param>
    /// <param name="outParamSpecifier">
    /// The output parameter builder.
    /// </param>
    /// <param name="resultSpecifier">
    /// The result builder.
    /// </param>
    member this.Proc<'Params1, 'Params2, 'Params3, 'Params4, 'Params5, 'OutParams, 'Result> (
            procName: string,
            name1: string, name2: string, name3: string, name4: string, name5: string,
            outParamSpecifier: OutParamSpecifier<'OutParams>, 
            resultSpecifier: ResultSpecifier<'Result>,
            [<CallerFilePath; Optional; DefaultParameterValue("")>] sourcePath: string,
            [<CallerLineNumber; Optional; DefaultParameterValue(0)>] sourceLine: int)
            : 'Params1 -> 'Params2 -> 'Params3 -> 'Params4 -> 'Params5 -> DbCall<'Result * 'OutParams> = 
        this.Proc(procName,
                  Params.Auto<'Params1>(name1), Params.Auto<'Params2>(name2), 
                  Params.Auto<'Params3>(name3), Params.Auto<'Params4>(name4), 
                  Params.Auto<'Params5>(name5),
                  outParamSpecifier,
                  resultSpecifier,
                  sourcePath, sourceLine)

    /// <summary>
    /// Builds a query function with five curried args invoking stored procedure.
    /// </summary>
    /// <param name="procName">
    /// The stored procedure name.
    /// </param>
    /// <param name="name1">
    /// The first parameter name.
    /// </param>
    /// <param name="name2">
    /// The second parameter name.
    /// </param>
    /// <param name="name3">
    /// The third parameter name.
    /// </param>
    /// <param name="name4">
    /// The fourth parameter name.
    /// </param>
    /// <param name="name5">
    /// The fifth parameter name.
    /// </param>
    /// <param name="sourcePath">
    /// The calling source path for diagnostic purposes.
    /// </param>
    /// <param name="sourceLine">
    /// The calling source path for diagnostic purposes.
    /// </param>
    /// <param name="outParamName">
    /// The output parameter name.
    /// </param>
    /// <param name="resultName">
    /// The result column name.
    /// </param>
    member this.Proc<'Params1, 'Params2, 'Params3, 'Params4, 'Params5, 'OutParams, 'Result> (
            procName: string,
            [<Optional>] name1: string, [<Optional>] name2: string, [<Optional>] name3: string, [<Optional>] name4: string, [<Optional>] name5: string,
            [<Optional>] outParamName: string, 
            [<Optional>] resultName: string,
            [<CallerFilePath; Optional; DefaultParameterValue("")>] sourcePath: string,
            [<CallerLineNumber; Optional; DefaultParameterValue(0)>] sourceLine: int)
            : 'Params1 -> 'Params2 -> 'Params3 -> 'Params4 -> 'Params5 -> DbCall<'Result * 'OutParams> = 
        this.Proc(procName, 
                  Params.Auto<'Params1>(name1), Params.Auto<'Params2>(name2), 
                  Params.Auto<'Params3>(name3), Params.Auto<'Params4>(name4), 
                  Params.Auto<'Params5>(name5),
                  OutParams.Auto<'OutParams>(outParamName), 
                  Results.Auto<'Result>(resultName), 
                  sourcePath, sourceLine)                