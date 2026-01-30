# Change: Fix Deployment and Complete Rebranding

## Why
The deployment is currently failing with `ModuleBootLoaderException` because the database tables (`AD_FCCONFIG`, etc.) are still registered under the old addon name (`iconicintegration`). Additionally, the `build.gradle` contains fragile manual file manipulations (`postDeployFix`) and hardcoded paths that make the build unreliable.

## What Changes
- **Database:** specific SQL script to migrate ownership of `AD_*` tables from `iconicintegration` to `fastchannelintegration`.
- **Build:** Refactor `build.gradle` to remove hardcoded paths (use local.properties or env vars) and clean up the manual `injectExtensionIntoWar` logic if possible, or at least make it robust.
- **Cleanup:** Remove the `extension 'snkmodule' property 'serverFolder'` artifact directory.

## Impact
- **Affected specs:** Deployment, Database Integration.
- **Affected code:** `build.gradle`, `dbscripts/`, `settings.gradle`.
