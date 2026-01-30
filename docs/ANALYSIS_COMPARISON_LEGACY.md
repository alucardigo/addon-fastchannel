# Análise Comparativa - Implementação vs Legado

**Data**: 2026-01-30
**Objetivo**: Comparar implementação atual com aplicação legado e código de referência VendaPixAdianta

## 🔍 Fontes Analisadas

1. **Legado**: `gbi-app-integrador-main/models/erp/sankhya.js` (Node.js)
2. **Referência**: `Modelo-Model/ejbsrc/br/com/bellube/sankhya/eventos/VendaPixAdianta/service/AdiantamentoService.java`
3. **Atual**: Nossa implementação em `SankhyaServiceInvoker.java` e `OrderXmlBuilder.java`

---

## 📊 Comparação - Legado Node.js

### ✅ O que o Legado Faz CERTO

```javascript
// Linha 90 e 180 do legado
path: '/mgecom/service.sbr?serviceName=CACSP.incluirNota&mgeSession=' + JSESSIONID
```

**1. Autenticação Completa:**
- Faz LOGIN no Sankhya primeiro (obtém JSESSIONID)
- Passa JSESSIONID na URL e no header
- Faz LOGOFF depois da operação

**2. Endpoint Correto:**
- Usa `/mgecom/service.sbr` ✅
- Serviço: `CACSP.incluirNota` ✅
- Método: POST com XML ✅

**3. XML Bem Estruturado:**
```xml
<serviceRequest serviceName="CACSP.incluirNota">
  <requestBody>
    <nota>
      <cabecalho>
        <TIPMOV>P</TIPMOV>
        <CODTIPVENDA>...</CODTIPVENDA>
        <CODNAT>...</CODNAT>
        <CODCENCUS>...</CODCENCUS>
        <CODPARC>...</CODPARC>
        <CODTIPOPER>...</CODTIPOPER>
        <CODEMP>...</CODEMP>
        <CODVEND>...</CODVEND>
        <VLRFRETE>...</VLRFRETE>
        <AD_NUMFAST>...</AD_NUMFAST>
      </cabecalho>
      <itens INFORMARPRECO="True">
        <item>
          <CODPROD>...</CODPROD>
          <CODVOL>...</CODVOL>
          <ORIGPROD>...</ORIGPROD>
          <VLRUNIT>...</VLRUNIT>
          <PERCDESC>...</PERCDESC>
          <QTDNEG>...</QTDNEG>
        </item>
      </itens>
    </nota>
  </requestBody>
</serviceRequest>
```

---

## 📊 Comparação - Referência VendaPixAdianta

### ✅ O que VendaPixAdianta Faz

**1. USA API Oficial do Sankhya:**
```java
AdiantamentoEmprestimoHelper helper = new AdiantamentoEmprestimoHelper();
DynamicVO despesaVO = helper.buildDespesaAdiantamento(dadosDespesa, false);
helper.salvarParcelamento(titulos, CODUSU_SISTEMA);
```

**2. NÃO USA ServiceInvoker nem HTTP:**
- Usa helpers especializados do Sankhya
- Trabalha diretamente com DynamicVO
- Deixa o helper cuidar de toda a lógica

**3. Configurações via Helpers:**
```java
BigDecimal codTop = ConfiguracaoHelper.getCodTopAdiantamento(codemp);
BigDecimal codTipTit = ConfiguracaoHelper.getCodTipTitAdiantamento(codemp);
BigDecimal codNat = ConfiguracaoHelper.getCodNatAdiantamento(codemp);
```

**Importante**: VendaPixAdianta cria **TÍTULOS FINANCEIROS**, não notas de venda, então a API é diferente.

---

## 🚨 PROBLEMAS CRÍTICOS na Nossa Implementação

### ❌ Problema 1: Falta de Autenticação

**Nossa implementação:**
```java
// SankhyaServiceInvoker.java - linha 58
HttpURLConnection conn = (HttpURLConnection) url.openConnection();
conn.setRequestMethod("POST");
// NÃO FAZ LOGIN - NÃO TEM JSESSIONID!
```

**Problema**:
- Chamada HTTP sem autenticação prévia
- Sankhya exige JSESSIONID válido para `/mge/service.sbr`
- Vai retornar erro 401 ou rejeitar requisição

### ❌ Problema 2: ServiceInvoker sem Contexto

**Nossa implementação:**
```java
// SankhyaServiceInvoker.java - linha 46
ServiceInvoker invoker = new ServiceInvoker(serviceName, requestXml);
String response = invoker.invoke();
```

**Problema Potencial**:
- ServiceInvoker precisa de contexto de usuário ativo
- Se não houver sessão Sankhya ativa, pode falhar
- Não há tratamento de contexto de autenticação

### ❌ Problema 3: URL Base Incorreta

**Nossa implementação:**
```java
// SankhyaServiceInvoker.java - linha 109
String serverUrl = "http://localhost:8080";
```

**Problemas**:
- Hardcoded localhost
- Legado usa configuração do contrato (`apiAddress`, `apiPort`, `apiProtocol`)
- Não considera contexto de deployment

### ❌ Problema 4: Estrutura do XML Ligeiramente Diferente

**Nossa implementação:**
```xml
<servicerequest serviceName="CACSP.incluirNota">
```

**Legado:**
```xml
<serviceRequest serviceName="CACSP.incluirNota">
```

Problema: Case-sensitive! `servicerequest` vs `serviceRequest` (R maiúsculo)

### ⚠️ Problema 5: Atributo `INFORMARPRECO` Ausente

**Legado:**
```xml
<itens INFORMARPRECO="True">
```

**Nossa implementação:**
```xml
<itens>
```

Problema: Atributo pode ser obrigatório para cálculo correto de preços

---

## ✅ O que Nossa Implementação Faz CERTO

1. **Validação de produtos ANTES de criar pedido** ✅
   - Legado só valida durante criação
   - Nossa implementação fail-fast

2. **Flag de sincronização** ✅
   - Controle fino de quando sincronizar
   - Legado não tem esse controle

3. **Resiliência** ✅
   - Falha de um pedido não afeta outros
   - Legado pode parar lote inteiro

4. **Estrutura de código mais limpa** ✅
   - Separação de responsabilidades
   - Fácil manutenção

5. **Fallback HTTP** ✅
   - Conceito correto
   - Implementação precisa de ajustes

---

## 🔧 CORREÇÕES NECESSÁRIAS

### 1. Implementar Login/Logout Sankhya

Criar `SankhyaAuthManager.java`:

```java
public class SankhyaAuthManager {
    public String login(String baseUrl, String username, String password) {
        // POST /mge/service.sbr?serviceName=MobileLoginSP.login
        // Retorna JSESSIONID
    }

    public void logout(String baseUrl, String jsessionId) {
        // POST /mge/service.sbr?serviceName=MobileLoginSP.logout
    }
}
```

### 2. Usar Login na Chamada HTTP

Modificar `SankhyaServiceInvoker.invokeViaHttp()`:

```java
// 1. Fazer login
String jsessionId = authManager.login(baseUrl, user, password);

// 2. Adicionar JSESSIONID na requisição
String fullUrl = baseUrl + SERVICE_URL_PATH +
                 "?serviceName=" + serviceName +
                 "&mgeSession=" + jsessionId;

conn.setRequestProperty("Cookie", "JSESSIONID=" + jsessionId);
conn.setRequestProperty("JSESSIONID", jsessionId);

// 3. Fazer requisição
// ...

// 4. Fazer logout
authManager.logout(baseUrl, jsessionId);
```

### 3. Corrigir Estrutura do XML

Modificar `OrderXmlBuilder.buildIncluirNotaXml()`:

```java
xml.append("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n");
xml.append("<serviceRequest serviceName=\"CACSP.incluirNota\">\n"); // R maiúsculo!
xml.append("  <requestBody>\n");
xml.append("    <nota>\n");

// ...cabecalho...

xml.append("      <itens INFORMARPRECO=\"True\">\n"); // Adicionar atributo!
```

### 4. Obter URL Base Corretamente

```java
private String getServerBaseUrl() {
    // 1. Tentar propriedade do sistema
    String serverUrl = System.getProperty("sankhya.server.url");

    // 2. Tentar variável de ambiente
    if (serverUrl == null) {
        serverUrl = System.getenv("SANKHYA_SERVER_URL");
    }

    // 3. Tentar detectar do contexto do servidor
    if (serverUrl == null) {
        try {
            // Usar InitialContext para pegar configuração do servidor
            serverUrl = lookupFromJNDI("java:comp/env/sankhyaServerUrl");
        } catch (Exception e) {
            // Fallback
        }
    }

    // 4. Último recurso - configuração do Fastchannel
    if (serverUrl == null) {
        FastchannelConfig config = FastchannelConfig.getInstance();
        serverUrl = config.getSankhyaServerUrl(); // ADICIONAR esse campo
    }

    return serverUrl;
}
```

### 5. Adicionar Credenciais no AD_FCCONFIG

Adicionar campos na tabela:
- `SANKHYA_USER` - Usuário do Sankhya para autenticação
- `SANKHYA_PASSWORD` - Senha (criptografada)
- `SANKHYA_SERVER_URL` - URL base do servidor Sankhya

---

## 🎯 RECOMENDAÇÕES PRIORITÁRIAS

### Prioridade CRÍTICA (Implementar AGORA)

1. ✅ **Adicionar login/logout no fluxo HTTP**
   - Sem isso, chamada HTTP vai FALHAR

2. ✅ **Corrigir case do XML**
   - `serviceRequest` (não `servicerequest`)
   - Adicionar `INFORMARPRECO="True"`

3. ✅ **Configurar URL base correta**
   - Adicionar campos no AD_FCCONFIG
   - Obter dinamicamente do ambiente

### Prioridade ALTA (Próxima iteração)

4. ⚠️ **Revisar ServiceInvoker**
   - Testar se funciona sem contexto web
   - Pode precisar de usuário autenticado no JapeSessionContext

5. ⚠️ **Adicionar credenciais de autenticação**
   - Usuário e senha para login
   - Gerenciamento seguro de credenciais

### Prioridade MÉDIA (Melhorias futuras)

6. 📝 **Considerar usar Helper do Sankhya**
   - Pesquisar se existe `NotaVendaHelper` ou similar
   - Pode ser mais robusto que chamada direta ao serviço

---

## 📚 Documentação Oficial Sankhya

### Serviços Conhecidos

1. **MobileLoginSP.login** - Autenticação
2. **MobileLoginSP.logout** - Encerrar sessão
3. **CACSP.incluirNota** - Incluir nota de venda ✅
4. **CACSP.alterarNota** - Alterar nota existente
5. **CRUDServiceProvider.loadRecords** - Buscar registros

### Estrutura de Resposta

```xml
<serviceResponse status="1">
  <responseBody>
    <pk>
      <NUNOTA>12345</NUNOTA>
    </pk>
  </responseBody>
</serviceResponse>
```

Status:
- `1` = Sucesso
- `0` = Erro (ler `statusMessage` com base64)

---

## ✅ CHECKLIST DE CONFORMIDADE

### Legado Node.js
- [x] Usa CACSP.incluirNota
- [x] Faz login antes
- [x] Passa JSESSIONID
- [x] Faz logout depois
- [x] XML bem estruturado
- [x] Mapeamento de campos completo
- [ ] Validação de produtos antecipada
- [ ] Controle de sincronização

### Nossa Implementação
- [x] Usa CACSP.incluirNota
- [ ] Faz login antes **❌ FALTANDO**
- [ ] Passa JSESSIONID **❌ FALTANDO**
- [ ] Faz logout depois **❌ FALTANDO**
- [ ] XML bem estruturado **⚠️ CASE ERRADO**
- [x] Mapeamento de campos completo
- [x] Validação de produtos antecipada ✅
- [x] Controle de sincronização ✅

### VendaPixAdianta (Referência)
- [x] Usa API oficial do Sankhya
- [x] Trabalha com DynamicVO
- [x] Usa helpers especializados
- [x] Configuração via helpers
- [x] Processamento assíncrono
- [x] Tratamento de erros robusto

---

## 🔐 Segurança

### Observações do Legado

1. **Credenciais**: Armazenadas em `activeContract.integracao.serverData`
2. **Senha**: Não criptografada no legado (vulnerabilidade)
3. **JSESSIONID**: Descartado após uso (correto)

### Recomendações

1. **Criptografar senha** no AD_FCCONFIG
2. **Usar variáveis de ambiente** para credenciais sensíveis
3. **Rotação de credenciais** periódica
4. **Audit log** de acessos ao Sankhya

---

## 🎬 CONCLUSÃO

### Status Atual
**⚠️ IMPLEMENTAÇÃO INCOMPLETA - NÃO VAI FUNCIONAR EM PRODUÇÃO**

### Motivo
Falta autenticação HTTP (login/logout com JSESSIONID). Sem isso, chamadas ao Sankhya via HTTP serão rejeitadas.

### Prioridade
**CRÍTICA** - Implementar ANTES de deploy em produção.

### Próximos Passos
1. Implementar SankhyaAuthManager
2. Integrar login/logout no fluxo HTTP
3. Adicionar campos de configuração no AD_FCCONFIG
4. Testar com servidor Sankhya real
5. Validar resposta e tratamento de erros

### Estimativa de Correção
- Login/Logout: ~2-3 horas
- Configuração: ~1 hora
- Testes: ~2-3 horas
- **Total: 5-7 horas de trabalho**

---

## 📝 NOTAS ADICIONAIS

### ServiceInvoker vs HTTP

O ServiceInvoker é ideal para uso INTERNO (dentro de eventos, ações), pois tem contexto de sessão automático. Para uso EXTERNO (schedulers, integrações), HTTP é mais confiável.

**Recomendação**: Manter ambos os fluxos, mas priorizar correção do HTTP.

### Helpers do Sankhya

Pesquisar se existe algum helper oficial para notas de venda (similar ao AdiantamentoEmprestimoHelper). Pode simplificar muito o código.

### Testes

Criar casos de teste para:
1. Login bem-sucedido
2. Login com credenciais inválidas
3. Timeout de sessão
4. Logout após uso
5. Pedido criado com sucesso
6. Pedido com erro (produto inexistente)
7. Pedido com erro de validação do Sankhya

---

**Autor**: Claude Sonnet 4.5
**Revisão**: Necessária por desenvolvedor senior Sankhya
