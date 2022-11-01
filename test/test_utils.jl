function get_template_basic_uc_simulation()
    template = ProblemTemplate(CopperPlatePowerModel)
    set_device_model!(template, ThermalStandard, ThermalBasicUnitCommitment)
    set_device_model!(template, RenewableDispatch, RenewableFullDispatch)
    set_device_model!(template, PowerLoad, StaticPowerLoad)
    set_device_model!(template, InterruptibleLoad, StaticPowerLoad)
    set_device_model!(template, HydroEnergyReservoir, HydroDispatchRunOfRiver)
    return template
end

function test_simulation_export(optimizer)
    template = get_template_basic_uc_simulation()
    set_service_model!(
        template,
        ServiceModel(VariableReserve{ReserveUp}, RangeReserve, "test"),
    )
    c_sys = PSB.build_system(PSITestSystems, "c_sys5_hy_uc")
    models = PSI.SimulationModels([
        PSI.DecisionModel(template, c_sys, name="UC", optimizer=optimizer),
    ])
    test_sequence =
        PSI.SimulationSequence(models=models, ini_cond_chronology=PSI.InterProblemChronology())
    sim = PSI.Simulation(
        name="consecutive",
        steps=2,
        models=models,
        sequence=test_sequence,
        simulation_folder=mktempdir(cleanup=true),
    )
    build_out = PSI.build!(sim)
    @test build_out == PSI.BuildStatus.BUILT
    execute_out = PSI.execute!(sim)
    @test execute_out == PSI.RunStatus.SUCCESSFUL

    results = SimulationResults(sim)
    results_uc = get_decision_problem_results(results, "UC")
    set_system!(results_uc, c_sys)

    export_dir = joinpath(results_uc.execution_path, "results")
    export_marmot_inputs(results_uc, export_dir)
    @test isfile(joinpath(export_dir, "generation_actual.csv"))
    @test isfile(joinpath(export_dir, "generation_availability.csv"))
    @test isfile(joinpath(export_dir, "generation_commitment.csv"))
    @test isfile(joinpath(export_dir, "installed_capacity.csv"))
    @test isfile(joinpath(export_dir, "regional_load.csv"))
    @test isfile(joinpath(export_dir, "metadata.json"))




    export_marmot_inputs(c_sys, joinpath(results_uc.execution_path, "results"))
end


function test_simulation_export(optimizer)
    template = get_template_basic_uc_simulation()
    set_service_model!(
        template,
        ServiceModel(VariableReserve{ReserveUp}, RangeReserve, "test"),
    )
    c_sys = PSB.build_system(PSITestSystems, "c_sys5_hy_uc")
    models = PSI.SimulationModels([
        PSI.DecisionModel(template, c_sys, name="UC", optimizer=optimizer),
    ])
    test_sequence =
        PSI.SimulationSequence(models=models, ini_cond_chronology=PSI.InterProblemChronology())
    sim = PSI.Simulation(
        name="consecutive",
        steps=2,
        models=models,
        sequence=test_sequence,
        simulation_folder=mktempdir(cleanup=true),
    )
    build_out = PSI.build!(sim)
    @test build_out == PSI.BuildStatus.BUILT
    execute_out = PSI.execute!(sim)
    @test execute_out == PSI.RunStatus.SUCCESSFUL

    results = SimulationResults(sim)
    results_uc = get_decision_problem_results(results, "UC")
    set_system!(results_uc, c_sys)
    variables = PSI.read_realized_variables(results_uc)
    aux_variables = PSI.read_realized_aux_variables(results_uc)
    parameters = PSI.read_realized_parameters(results_uc)
    duals = PSI.read_realized_duals(results_uc)
    expressions = PSI.read_realized_expressions(results_uc)

    export_dir = joinpath(results_uc.execution_path, "results")
    export_marmot_inputs(results_uc, export_dir)

    generation_actual_path = joinpath(export_dir, "generation_actual.csv")
    @test isfile(generation_actual_path)
    test_generation_actual(variables, aux_variables, generation_actual_path)

    generation_availability_path = joinpath(export_dir, "generation_availability.csv")
    @test isfile(generation_availability_path)
    test_generation_availability(parameters, generation_availability_path)

    generation_commitment_path = joinpath(export_dir, "generation_commitment.csv")
    @test isfile(generation_commitment_path)
    test_generation_commitment(variables, generation_commitment_path)

    installed_capacity_path = joinpath(export_dir, "installed_capacity.csv")
    @test isfile(installed_capacity_path)
    test_installed_capacity(c_sys, installed_capacity_path)

    regional_load_path = joinpath(export_dir, "regional_load.csv")
    @test isfile(regional_load_path)
    test_regional_load(results_uc, regional_load_path)

    metadata_path = joinpath(export_dir, "metadata.json")
    @test isfile(metadata_path)


end

function test_problem_export(optimizer)
    c_sys5 = PSB.build_system(PSITestSystems, "c_sys5")
    template = get_template_basic_uc_simulation()
    set_service_model!(
        template,
        ServiceModel(VariableReserve{ReserveUp}, RangeReserve, "test"),
    )
    UC = DecisionModel(template, c_sys5; optimizer=optimizer)
    output_dir = mktempdir(cleanup=true)
    @test build!(UC; output_dir=output_dir) == PSI.BuildStatus.BUILT
    @test solve!(UC; optimizer=optimizer) == RunStatus.SUCCESSFUL
    res = ProblemResults(UC)

    set_system!(results_uc, c_sys)

    export_dir = joinpath(results_uc.execution_path, "results")
    export_marmot_inputs(results_uc, export_dir)

    generation_actual_path = joinpath(export_dir, "generation_actual.csv")
    @test isfile(generation_actual_path)
    test_generation_actual(variables, aux_variables, generation_actual_path)

    generation_availability_path = joinpath(export_dir, "generation_availability.csv")
    @test isfile(generation_availability_path)
    test_generation_availability(parameters, generation_availability_path)

    generation_commitment_path = joinpath(export_dir, "generation_commitment.csv")
    @test isfile(generation_commitment_path)
    test_generation_commitment(variables, generation_commitment_path)

    installed_capacity_path = joinpath(export_dir, "installed_capacity.csv")
    @test isfile(installed_capacity_path)
    test_installed_capacity(c_sys, installed_capacity_path)

    regional_load_path = joinpath(export_dir, "regional_load.csv")
    @test isfile(regional_load_path)
    test_regional_load(results_uc, regional_load_path)

    metadata_path = joinpath(export_dir, "metadata.json")
    @test isfile(metadata_path)

end


function test_sys_export(optimizer)
    c_sys5 = PSB.build_system(PSITestSystems, "c_sys5")
    export_dir = mktempdir(cleanup=true)
    export_marmot_inputs(c_sys5, export_dir)

    installed_capacity_path = joinpath(export_dir, "installed_capacity.csv")
    @test isfile(installed_capacity_path)
    test_installed_capacity(c_sys, installed_capacity_path)
end


function test_generation_actual(variables, aux_variables, path)
    active_power_dataframes = filter(x -> startswith(x[1], "ActivePowerVariable") || startswith(x[1], "ActivePowerOutVariable"), variables)
    aux_active_power_dataframes = filter(x -> startswith(x[1], "PowerOutput"), aux_variables)
    col_names = []
    for (_, df) in merge(aux_active_power_dataframes, active_power_dataframes)
        push!(col_names, DataFrames.names(df)...)
    end
    test_csv_columns(path, col_names)

end

function test_generation_availability(parameters, path)
    active_power_dataframes = filter(x -> startswith(x[1], "ActivePowerTimeSeriesParameter") && !occursin("PowerLoad", x[1]), parameters)
    col_names = []
    for (_, df) in active_power_dataframes
        push!(col_names, DataFrames.names(df)...)
    end
    test_csv_columns(path, col_names)
end

function test_generation_commitment(variables, path)
    active_power_dataframes = filter(x -> startswith(x[1], "OnVariable"), variables)
    col_names = []
    for (_, df) in active_power_dataframes
        push!(col_names, DataFrames.names(df)...)
    end
    test_csv_columns(path, col_names)
end

function test_installed_capacity(system, path)
    col_names = ["DateTime"]
    for gen_type in [PSY.ThermalGen, PSY.RenewableGen, PSY.HydroGen, PSY.Storage, PSY.HybridSystem]
        push!(col_names, PSY.get_name.(PSY.get_components(gen_type, system, PSY.get_available))...)
    end
    test_csv_columns(path, col_names)
end

function test_regional_load(results, path)
    if !isempty(PSY.get_components(PSY.Area, results.system))
        col_names = PSY.get_name.(PSY.get_components(PSY.Area, results.system))
    else
        col_names = ["DateTime", "System"]
    end
    test_csv_columns(path, col_names)
end

function test_csv_columns(path, names)
    df = DataFrame(CSV.File(path))
    _col_names = DataFrames.names(df)
    @test isempty(setdiff(_col_names, names))
end
