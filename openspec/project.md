# Project Context

## Purpose
Integration project between Sankhya ERP and Fastchannel (e-commerce/marketplace integrator).
The solution involves a Sankhya extension (Java/Kotlin) and likely a middleware/integrator app (Node.js).

## Tech Stack
### Backend (Sankhya Extension)
- **Language:** Java, Kotlin
- **Build Tool:** Gradle
- **Framework:** SankhyaW Extensions (JAPE, ModelCore)
- **Deployment:** EAR/WAR (JBoss/Wildfly)

### Middleware / Integrator
- **Directory:** `gbi-app-integrador-main`
- **Language:** JavaScript/Node.js
- **Package Manager:** pnpm (inferred from `pnpm-lock.yaml`)
- **Containerization:** Docker (`Dockerfile`, `docker-compose.yml`)

## Project Conventions

### Code Style
- Follow standard Java/Kotlin conventions for the extension.
- Follow standard JavaScript/Node.js conventions for the integrator app.
- Check `.editorconfig` for specific formatting rules.

### Architecture Patterns
- **Sankhya Extension:** Follows Sankhya's module structure (`model`, `vc`, `WEB-INF`). Uses Listeners, Services, and Scheduled Jobs (`skJob`, `skListener`, `skService`).
- **Integrator:** likely an Express.js or similar web server (`app.js`, `server.sh`).

### Database
- SQL scripts located in `dbscripts`.
- Uses XML based data dictionaries (`datadictionary/*.xml`).

## Domain Context
- **Sankhya:** ERP system.
- **Fastchannel:** E-commerce integration platform.
- Key Entities: Pedidos (Orders), Produtos (Products), Clientes (Customers), Queues (FCQUEUE), Logs (FCLOG).

## Important Constraints
- Modifications to Sankhya native tables should be avoided unless necessary; prefer custom tables (`AD_*`).
- Deployments involve building EAR/WAR files and placing them in the server deployment folder.

## External Dependencies
- SankhyaW Extension API
- Fastchannel API