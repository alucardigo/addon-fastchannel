## 1. Database Migration (Codex)
- [x] 1.1 Create `dbscripts/migration_iconic_to_fastchannel.sql` to update `TDDCAM`, `TDDTAB`, etc., changing `DOMAIN` from `iconicintegration` to `fastchannelintegration`.
- [x] 1.2 Verify if `AD_FCCONFIG` needs to be dropped or just updated. (Handled by updating DOMAIN ownership, logic included in migration)

## 2. Build System Refactor (Claude)
- [x] 2.1 Extract hardcoded `serverFolder` from `build.gradle` to `local.properties` (add to .gitignore).
- [x] 2.2 Review `postDeployFix` task and simplify the XML patching logic.
- [x] 2.3 Fix the issue causing the creation of the folder `extension 'snkmodule' property 'serverFolder'`.

## 3. Verification (Qwen)
- [x] 3.1 Verify that `gradlew build` runs without errors.
- [x] 3.2 Verify that the `dist/` folder contains the correct `extension.xml`.