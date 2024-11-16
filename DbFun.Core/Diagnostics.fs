namespace DbFun.Core

open System 

module Diagnostics = 

    /// <summary>
    /// The exception occurring in the code generation phase.
    /// </summary>
    type CompileTimeException(message: string, innerException: Exception) = 
        inherit Exception(message, innerException)

    /// <summary>
    /// The compile time error log entry.
    /// </summary>
    type CompileTimeErrorLog = list<int * string * exn>


    /// <summary>
    /// The exception occurring in the command execution phase.
    /// </summary>
    type RuntimeException(message: string, innerException: Exception) = 
        inherit Exception(message, innerException)
