namespace DbFun.TestTools

open DbFun.Core

module Templating = 

    let where = Templating.expand "WHERE-CLAUSE" " where " " and "
    let orderBy = Templating.expand "ORDER-BY-CLAUSE" " order by " ", "
    let join = Templating.expand "JOIN-CLAUSES" " " " "
    let groupBy = Templating.expand "GROUP-BY-CLAUSE" "group by " ", "
    let having = Templating.expand "HAVING-CLAUSE" "having " " and "
