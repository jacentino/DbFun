namespace DbFun.Core

open System 

module Diagnostics = 

    type CompileTimeException(message: string, innerException: Exception) = 
        inherit Exception(message, innerException)

    type CompileTimeErrorLog = list<int * string * exn>

   