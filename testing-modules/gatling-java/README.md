### Running a simulation

To run the simulations from command prompt use `mvn gatling:test`. This will trigger all 3 simulations: EmployeeRegistrationSimulation, FetchSinglePostSimulation and FetchSinglePostSimulationLog.

For executing any other simulations, use `mvn gatling:test -Dgatling.simulationClass=org.baeldung.FastEndpointSimulation`
