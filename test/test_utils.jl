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
    c_sys = PSB.build_system(PSITestSystems, "c_sys5_uc")
    models = PSI.SimulationModels([
        PSI.DecisionModel(template, c_sys, name="UC", optimizer=optimizer),
    ])
    test_sequence =
    PSI.SimulationSequence(models=models, ini_cond_chronology=PSI.InterProblemChronology())
    sim  = PSI.Simulation(
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
    set_system!(results_uc, c_sys);

    export_marmot_inputs(results_uc, joinpath(results_uc.execution_path,"results"))
    # @test isfile
end


function  test_problem_export(optimizer)
    c_sys5 = PSB.build_system(PSITestSystems, "c_sys5")
    template = get_template_basic_uc_simulation()
    set_service_model!(
        template,
        ServiceModel(VariableReserve{ReserveUp}, RangeReserve, "test"),
    )
    UC = DecisionModel(template, c_sys5; optimizer=GLPK_optimizer)
    output_dir = mktempdir(cleanup=true)
    @test build!(UC; output_dir=output_dir) == PSI.BuildStatus.BUILT
    @test solve!(UC; optimizer=GLPK_optimizer) == RunStatus.SUCCESSFUL
    res = ProblemResults(UC)

    set_system!(results_uc, c_sys);

    export_marmot_inputs(results_uc, joinpath(results_uc.execution_path,"results"))
end
