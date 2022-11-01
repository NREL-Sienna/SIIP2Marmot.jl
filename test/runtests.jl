using Test
using Dates
using PowerSystemCaseBuilder
using PowerSystems
using PowerSimulations
using Cbc
using JuMP
using Arrow
using Dates
using DataFrames
using CSV
using SIIP2Marmot
const PSY = PowerSystems
const PSI = PowerSimulations
const PSB = PowerSystemCaseBuilder

optimizer = JuMP.optimizer_with_attributes(Cbc.Optimizer)

include("./test_utils.jl")

test_simulation_export(optimizer)
test_problem_export(optimizer)
test_sys_export(optimizer)


