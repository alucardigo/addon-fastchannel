## ADDED Requirements
### Requirement: Clean Deployment
The system MUST deploy to Wildfly without `ModuleBootLoaderException`.

#### Scenario: Successful Deploy
- **WHEN** `gradlew deployAddon` is executed
- **THEN** the server log MUST show "Deployed FastchannelIntegration.ear"
- **AND** no conflicts with "iconicintegration" MUST appear.

### Requirement: Environment Configuration
The system MUST allow configuration of the deployment path without modifying versioned files.

#### Scenario: Local Properties
- **WHEN** a `local.properties` file exists with `serverFolder` defined
- **THEN** the build script MUST use this path instead of the hardcoded one.
